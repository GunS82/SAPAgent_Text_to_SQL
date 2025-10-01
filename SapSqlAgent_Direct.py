# direct_runner_json.py
import json
import os
from typing import List, Literal, Optional, Dict, Any

from pydantic import BaseModel, Field, ValidationError  # валидируем JSON ответа [web:7]
from db_logger import DBLogger  # запись в SQLite (dialog_message, dialog_log) [web:2]
from ws_session import NlmkChatSession  # WS-сессия с ассистентом [web:2]
from utils import extract_json_object, build_incremental_payload

# ===== JSON-модели, заданные заказчиком =====
class SolutionChecklist(BaseModel):
    tables_to_query: List[str]
    columns_to_query: List[str]
    dependency_kind: Literal["direct", "indirect", "N/A"]
    is_subject_system_from_or_to: Literal[
        "from_system_id points to our system",
        "to_system_id points to our system",
        "N/A"
    ] = Field(default=..., description="Are we looking for systems that depend on the target system (from_system_id) or systems that the target system depends on (to_system_id)?")
    does_this_require_recursive_query: bool
    does_this_require_subquery: bool
    is_this_forward_or_backward_pass: Literal["forward", "backward", "N/A"] = Field(default=..., description="Determines if we start with .to_system_id or .from_system_id?")
    should_we_filter_out_subject_system_from_results_to_avoid_overcounting: bool

class Response(BaseModel):
    strategy: SolutionChecklist
    sql_query: str

# Сериализуем JSON Schema в промпт — это повышает шанс корректного формата [web:131]
SCHEMA_JSON = json.dumps(Response.model_json_schema(), ensure_ascii=False, indent=2)  # [web:7][web:131]

SYSTEM_PROMPT = f"""
Вы — помощник по данным SAP. Верните ответ СТРОГО в формате JSON, без пояснений и Markdown, соответствующий следующей схеме Pydantic (Response):
<JSON SCHEMA>
{SCHEMA_JSON}
</JSON SCHEMA>
ПРАВИЛА РАБОТЫ:
- CDS/HANA views не использовать. Z* не предлагать.
- Возвращайте только один JSON-объект Response.
- В поле sql_query поместите полноценный SQL-запрос, который можно выполнить как есть.
- Не добавляйте текст до или после JSON.
""".strip()

def run_direct_llm_json_and_log(
    nl_query: str,
    base_url: str = os.getenv("BASE_URL"),
    ws_url: str = os.getenv("WS_URL"),
    api_key: str = os.getenv("API_KEY"),
    assistant_id: Optional[str] = os.getenv("Assistant"),
) -> Dict[str, Any]:
    """
    Делает один прямой вызов LLM с требованием JSON по модели Response,
    валидирует Pydantic-ом и сохраняет историю и финал в те же таблицы SQLite.
    """
    if assistant_id is None:
        assistant_id = os.getenv("ASSISTANT_ID")
    if not api_key or not assistant_id:
        raise ValueError("Требуются API_KEY и ASSISTANT_ID")

    # 1) WS-сессия
    chat = NlmkChatSession(base_url=base_url, ws_url=ws_url, api_key=api_key, assistant_id=assistant_id)  # [web:2]
    chat.connect()  # [web:2]

    # 2) БД-логгер
    db = DBLogger()
    db.connect()  # создаёт таблицы и включает FK на соединении [web:2][web:48]
    dialog_id = db.reserve_dialog(nl_query) 
    # 3) История и лог в dialog_message
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": nl_query},
    ]
    db.log_message(turn_index=0, role="system", content=SYSTEM_PROMPT, meta={"mode": "direct-json"}, dialog_id=dialog_id)
    db.log_message(turn_index=1, role="user", content=nl_query, dialog_id=dialog_id)
    try:
        # 4) Отправляем один пакет и получаем потоковый ответ
        # Передаем только новую часть диалога (user), system уже в памяти ассистента.
        payload = build_incremental_payload(messages, 0)
        resp_text = chat.send_and_wait(payload, timeout_sec=180)  # [web:2]
        messages.append({"role": "assistant", "content": resp_text})
        db.log_message(turn_index=2, role="assistant", content=resp_text, meta={"raw_stream": True})  # [web:2]

        # 5) Жёсткий парсинг JSON + Pydantic-валидация
        json_obj = extract_json_object(resp_text)  # достаём объект из текста [web:2]
        if json_obj is None:
            raise ValueError("Ответ не содержит валидного JSON Response.")
        try:
            resp_model = Response.model_validate(json_obj)  # строгая проверка по модели [web:7]
        except ValidationError as e:
            # Попытка повторной валидации из чистой строки, если LLM прислал строку JSON
            try:
                resp_model = Response.model_validate_json(json.dumps(json_obj, ensure_ascii=False))  # [web:7][web:131]
            except Exception:
                raise ValueError(f"JSON не соответствует схеме Response: {e}")

        # 6) Формируем финальные поля для dialog_log:
        # - intent_summary: кратко по стратегии
        # - sql_used: сам SQL из модели
        # - result_summary: структурная сводка стратегии
        # - confidence: простая эвристика по наличию непустого sql_query
        strategy = resp_model.strategy
        intent_summary = f"Прямой JSON-ответ: {strategy.dependency_kind}, recursive={strategy.does_this_require_recursive_query}, subquery={strategy.does_this_require_subquery}"[:400]
        sql_used = resp_model.sql_query.strip()
        result_summary = (
            f"tables={', '.join(strategy.tables_to_query)}; "
            f"columns={', '.join(strategy.columns_to_query)}; "
            f"flow={strategy.is_this_forward_or_backward_pass}; "
            f"subject_mode={strategy.is_subject_system_from_or_to}; "
            f"filter_subject={strategy.should_we_filter_out_subject_system_from_results_to_avoid_overcounting}"
        )[:2000]
        confidence = 0.7 if sql_used else 0.3

        answer = {
            "intent_summary": intent_summary,
            "sql_used": sql_used or "DIRECT_JSON_EMPTY_SQL",
            "result_summary": result_summary,
            "confidence": confidence,
        }

        # 7) Фиксируем финальную строку в dialog_log и backfill dialog_id
        db.update_dialog(dialog_id, answer) 

        # 8) Сохраняем финал как сообщение для симметрии
        final_msg = json.dumps({"strategy": strategy.model_dump(), "sql_query": sql_used}, ensure_ascii=False)
        messages.append({"role": "assistant", "content": final_msg})
        db.log_message(turn_index=3, role="assistant", content=final_msg, meta={"final_answer": True}, dialog_id=dialog_id)  # [web:2]

        # Возвращаем структуру для удобства
        return {
            "final_answer": answer,
            "response_json": resp_model.model_dump(),
            "history": messages,
        }
    finally:
        chat.disconnect()
        db.close()

if __name__ == "__main__":
    import sys
    query = " ".join(sys.argv[1:]) or os.getenv("QUERY", "Выведи список  входящих IDOC  из системы HYB_PROD с распределением по типам сообщений (MESTYP) на даты   с 20.07.2025 по  24.07.2025.")
    out = run_direct_llm_json_and_log(query)
    print(json.dumps(out, indent=2, ensure_ascii=False))
