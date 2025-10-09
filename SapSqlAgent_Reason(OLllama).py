# main.py
# Агент для преобразования NL запросов в SQL для SAP через OpenAI-совместимый API

import json
import os
import sys
from typing import Any, Dict, List, Literal, Optional, Union
from annotated_types import Ge, Le, MaxLen, MinLen, Annotated
from pydantic import BaseModel, Field, ValidationError
import httpx
from openai import OpenAI

from sap_tools import run_sap_sql_query, are_tables_present, get_table_fields, get_domain_texts
from db_logger import DBLogger
from utils import extract_json_object

# ===== СХЕМЫ =====
class Tool_GetTableFields(BaseModel):
    tool: Literal["gettablefields"]
    table_name: Annotated[str, MinLen(1), MaxLen(40)]

class Tool_GetDomainTexts(BaseModel):
    tool: Literal["get_domain_texts"]
    domain_name: Annotated[str, MinLen(1), MaxLen(40)]

class Tool_RunSapSqlQuery(BaseModel):
    tool: Literal["runsapsql_query"]
    query: Annotated[str, MinLen(10)]
    name: Optional[Annotated[str, MinLen(2), MaxLen(40)]] = None

class FinalAnswer(BaseModel):
    intent_summary: Annotated[str, MinLen(8), MaxLen(400)]
    sql_used: Annotated[str, MinLen(10)]
    result_summary: Annotated[str, MinLen(8), MaxLen(2000)]
    confidence: Annotated[float, Ge(0.0), Le(1.0)]

class Step_SelectTables(BaseModel):
    kind: Literal["select_tables"]
    thought: Annotated[str, MinLen(10)]
    tables_to_verify: Annotated[List[str], MinLen(1)]

class Step_ExploreAndProbe(BaseModel):
    kind: Literal["explore_and_probe"]
    thought: Annotated[str, MinLen(10)]
    actions: Annotated[List[Union[Tool_GetTableFields, Tool_RunSapSqlQuery, Tool_GetDomainTexts]], MinLen(1)]

class Step_ExecuteFinalQuery(BaseModel):
    kind: Literal["execute_final_query"]
    thought: Annotated[str, MinLen(10)]
    final_sql: Annotated[str, MinLen(10)]

class Step_ProvideFinalAnswer(BaseModel):
    kind: Literal["provide_final_answer"]
    answer: FinalAnswer

class NextStep(BaseModel):
    next_step: Union[Step_SelectTables, Step_ExploreAndProbe, Step_ExecuteFinalQuery, Step_ProvideFinalAnswer]

# ===== ПРОМПТ =====
SCHEMA_JSON = json.dumps(NextStep.model_json_schema(), indent=2, ensure_ascii=False)

SYSTEM_PROMPT = f"""
Ты ассистент по SAP (ECC/S/4). Преобразуй запрос пользователя в SQL пошагово. База данных - Hana DB2.
На каждом ходе верни РОВНО ОДИН JSON по схеме .

ИНСТРУМЕНТЫ:
- are_tables_present
- get_table_fields
- run_sap_sql_query
- get_domain_texts

ПРАВИЛА РАБОТЫ:
- CDS/HANA views не использовать. Z* не предлагать.
- При сомнениях существования таблиц — вызывай select_tables, для анализа полей таблиц — gettablefields, поиска идентификаторов доменных значений по тексту - get_domain_texts.
- Разрешены пробные запуски в процессе размышления run_sap_sql_query. Для пробных запусков — всегда использовать ORDER BY для детерминированности и LIMIT для безопасности!!
- Финальный SQL - выполняется отдельно , без ограничений.

ВСПОМОГАТЕЛЬНАЯ ИНФОРМАЦИЯ ДЛЯ ПОИСКА ОТВЕТА:
- У многих объектов в системе есть основная запись(header), и позиции, подпозиции, статусы итп. При поиске и связях не забываем группировать по основному номеру, если это требуется.
- Для SAP полей типа NUMC используй полную длину с ведущими нулями
- Если ставишь проверки по доменным идентификаторам, то сначала уточни их значение
- При анализе полей таблиц обращай внимание на DOMNAME - это домен, и на CHECKTABLE - проверочные таблицы для поля. ENTITYTAB - там может быть таблица значений домена.

ВОЗВРАЩАЙ ТОЛЬКО JSON ПО СХЕМЕ.

{SCHEMA_JSON}
""".strip()

# ===== ФУНКЦИИ ВЫВОДА =====
def clear_console():
    """Очищает консоль в зависимости от ОС"""
    if sys.platform.startswith('win'):
        os.system('cls')
    else:
        os.system('clear')

def print_query(query: str):
    """Печатает исходный запрос пользователя"""
    print(f"\n🔍 Запрос: {query}\n")

def print_step_header(step_num: int):
    """Печатает заголовок шага"""
    print(f"\n▶ ШАГ {step_num}")

def print_thought(thought: str):
    """Печатает мысль агента"""
    print(f"💭 Размышление: {thought}")

def print_tool_call(tool_name: str, params: Dict[str, Any]):
    """Печатает вызов инструмента"""
    print(f"🔧 Вызов инструмента: {tool_name}")
    for key, value in params.items():
        if key == "sql" or key == "query":
            print(f"   📝 {key}: {value}")
        else:
            print(f"   • {key}: {value}")

def print_final_answer(answer: Dict[str, Any]):
    """Печатает финальный ответ красиво и структурированно"""
    print(f"\n✨ ФИНАЛЬНЫЙ ОТВЕТ\n")
    print(f"📋 Суть запроса: {answer['intent_summary']}\n")
    print(f"💾 Использованный SQL: {answer['sql_used']}\n")
    print(f"📊 Результат: {answer['result_summary']}\n")
    print(f"🎯 Уверенность: {answer['confidence']*100:.1f}%\n")

# ===== КЛИЕНТ OPENAI =====
def create_openai_client(base_url: str, api_key: Optional[str] = None) -> OpenAI:
    """Создание OpenAI клиента для Ollama/совместимого API"""
    if not api_key:
        api_key = "ollama"

    # Создаём http клиент с отключённой проверкой SSL
    http_client = httpx.Client(verify=False)

    return OpenAI(
        base_url=base_url.rstrip("/") + "/v1",
        api_key=api_key,
        http_client=http_client
    )

def stream_chat_completion(client: OpenAI, model: str, messages: List[Dict[str, str]], timeout: int = 180) -> str:
    """Выполняет streaming запрос и возвращает полный ответ"""
    try:
        stream = client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True,
            timeout=timeout
        )

        full_response = ""
        for chunk in stream:
            if chunk.choices and len(chunk.choices) > 0:
                delta = chunk.choices[0].delta
                if delta.content is not None:
                    full_response += delta.content

        return full_response

    except Exception as e:
        raise RuntimeError(f"Ошибка при запросе к API: {str(e)}")

# ===== АГЕНТ =====
def run_sgr_agent_adaptive(
    nl_query: str,
    max_steps: int = 20,
    base_url: str = os.getenv("OLLAMA_BASE_URL"),
    api_key: str = os.getenv("OLLAMA_API_KEY"),
    model: str = os.getenv("OLLAMA_MODEL"),
):
    """
    Запускает агент для преобразования NL запроса в SQL через OpenAI-совместимый API

    Args:
        nl_query: Естественно-языковой запрос пользователя
        max_steps: Максимальное количество шагов агента
        base_url: URL Ollama/OpenAI-совместимого API
        api_key: API ключ (опционально для Ollama)
        model: Имя модели (по умолчанию "ChatAI GPT-4.1 mini")
    """

    # Очистка консоли и вывод запроса в начале
    clear_console()
    print_query(nl_query)

    # Создание клиентов
    client = create_openai_client(base_url, api_key)
    db = DBLogger()
    db.connect()

    # Инициализация истории сообщений
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Задача: {nl_query}"},
    ]

    db.log_message(turn_index=0, role="system", content=SYSTEM_PROMPT, meta={"kind": "system_prompt"})
    db.log_message(turn_index=1, role="user", content=f"Задача: {nl_query}")

    bad_json_streak = 0
    final_dialog_id: Optional[int] = None
    step_counter = 0

    try:
        for iteration in range(1, max_steps + 1):
            # Отправка полной истории сообщений в API
            resp_text = stream_chat_completion(client, model, messages, timeout=180)

            messages.append({"role": "assistant", "content": resp_text})
            db.log_message(
                turn_index=len(messages) - 1, 
                role="assistant", 
                content=resp_text, 
                meta={"raw_stream": True, "iteration": iteration}, 
                dialog_id=final_dialog_id
            )

            # Извлечение JSON из ответа
            job = extract_json_object(resp_text)
            if not job:
                bad_json_streak += 1
                correction = "Ответ невалиден. Верни строго один JSON по схеме NextStep."
                messages.append({"role": "user", "content": correction})
                db.log_message(
                    turn_index=len(messages) - 1, 
                    role="user", 
                    content=correction, 
                    meta={"reason": "bad_json"}, 
                    dialog_id=final_dialog_id
                )
                if bad_json_streak >= 3:
                    example = {
                        "next_step": {
                            "kind": "select_tables",
                            "thought": "Пояснение…",
                            "tables_to_verify": ["VBRK", "VBRP"]
                        }
                    }
                    hint = f"Ответ невалиден. Верни JSON по схеме. Пример:\n```json\n{json.dumps(example, ensure_ascii=False, indent=2)}\n```"
                    messages.append({"role": "user", "content": hint})
                    db.log_message(
                        turn_index=len(messages) - 1, 
                        role="user", 
                        content=hint, 
                        meta={"reason": "bad_json_example"}, 
                        dialog_id=final_dialog_id
                    )
                continue

            # Валидация схемы
            try:
                plan = NextStep(**job)
            except ValidationError as e:
                bad_json_streak += 1
                correction = f"Ошибка валидации JSON: {str(e)}. Верни корректный JSON по схеме."
                messages.append({"role": "user", "content": correction})
                db.log_message(
                    turn_index=len(messages) - 1, 
                    role="user", 
                    content=correction, 
                    meta={"reason": "validation_error", "error": str(e)}, 
                    dialog_id=final_dialog_id
                )
                if bad_json_streak >= 3:
                    example = {
                        "next_step": {
                            "kind": "select_tables",
                            "thought": "Пояснение…",
                            "tables_to_verify": ["VBRK", "VBRP"]
                        }
                    }
                    hint = f"Ответ невалиден. Верни JSON по схеме. Пример:\n```json\n{json.dumps(example, ensure_ascii=False, indent=2)}\n```"
                    messages.append({"role": "user", "content": hint})
                    db.log_message(
                        turn_index=len(messages) - 1, 
                        role="user", 
                        content=hint, 
                        meta={"reason": "validation_error_example"}, 
                        dialog_id=final_dialog_id
                    )
                continue

            bad_json_streak = 0
            step = plan.next_step

            # Нормализация и логирование
            normalized = json.dumps(job, ensure_ascii=False, indent=2)
            messages.append({"role": "assistant", "content": normalized})
            db.log_message(
                turn_index=len(messages) - 1, 
                role="assistant", 
                content=normalized, 
                meta={"normalized": True}, 
                dialog_id=final_dialog_id
            )

            # Вывод текущего шага
            step_counter += 1
            print_step_header(step_counter)

            tool_results: List[Dict[str, Any]] = []

            if isinstance(step, Step_SelectTables):
                print_thought(step.thought)
                names = list(dict.fromkeys([t.upper() for t in step.tables_to_verify]))
                print_tool_call("are_tables_present", {"tables": names})
                result = are_tables_present(names)
                tool_results.append({"tool": "aretablespresent", "input": names, "result": result})

            elif isinstance(step, Step_ExploreAndProbe):
                print_thought(step.thought)
                for action in step.actions:
                    if isinstance(action, Tool_GetTableFields):
                        print_tool_call("get_table_fields", {"table_name": action.table_name})
                        result = get_table_fields(action.table_name)
                        tool_results.append({"tool": "gettablefields", "table": action.table_name, "result": result})
                    elif isinstance(action, Tool_GetDomainTexts):
                        print_tool_call("get_domain_texts", {"domain_name": action.domain_name})
                        result = get_domain_texts(action.domain_name)
                        tool_results.append({"tool": "get_domain_texts", "domain": action.domain_name, "result": result})
                    elif isinstance(action, Tool_RunSapSqlQuery):
                        params = {"query": action.query}
                        if action.name:
                            params["name"] = action.name
                        print_tool_call("run_sap_sql_query", params)
                        result = run_sap_sql_query(action.query)
                        tool_results.append({
                            "tool": "runsapsql_query", 
                            "name": action.name, 
                            "sql": action.query, 
                            "result": result
                        })

            elif isinstance(step, Step_ExecuteFinalQuery):
                print_thought(step.thought)
                print_tool_call("final_sql_execution", {"sql": step.final_sql})
                result = run_sap_sql_query(step.final_sql)
                tool_results.append({"tool": "final_sql_execution", "sql": step.final_sql, "result": result})

            elif isinstance(step, Step_ProvideFinalAnswer):
                final_dialog_id = db.log_final_answer(nl_query, step.answer.model_dump())
                db.backfill_dialog_id(final_dialog_id)
                final_msg = json.dumps(step.answer.model_dump(), ensure_ascii=False)
                messages.append({"role": "assistant", "content": final_msg})
                db.log_message(
                    turn_index=len(messages) - 1, 
                    role="assistant", 
                    content=final_msg, 
                    meta={"final_answer": True}, 
                    dialog_id=final_dialog_id
                )

                # Вывод финального ответа
                print_final_answer(step.answer.model_dump())

                return {"final_answer": step.answer.model_dump(), "history": messages}

            # Отправка результатов инструментов обратно в модель
            if tool_results:
                blob = json.dumps(tool_results, ensure_ascii=False, indent=2)
                messages.append({"role": "user", "content": f"Результаты инструментов:\n{blob}"})
                db.log_message(
                    turn_index=len(messages) - 1, 
                    role="user", 
                    content=blob, 
                    meta={"tool_results": True},
                    dialog_id=final_dialog_id
                )

        raise TimeoutError("Лимит шагов исчерпан без финального ответа.")

    finally:
        db.close()

if __name__ == "__main__":
    query = os.getenv("QUERY", "Сколько есть авиарейсов из Нью-Йорка?")
    try:
        out = run_sgr_agent_adaptive(query)
    except Exception as e:
        print(f"\n❌ ОШИБКА: {str(e)}\n")
        import traceback
        traceback.print_exc()
