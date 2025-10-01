# db_logger.py
import json
import sqlite3
import time
from typing import Optional, Dict, Any

class DBLogger:
    def __init__(self, db_path: str = "sgr_logs.sqlite3"):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self):
        if self.conn:
            return
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")  # [web:48]
        self._ensure_schema()

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            finally:
                self.conn = None

    def _ensure_schema(self):
        assert self.conn is not None
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dialog_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                nl_query TEXT NOT NULL,
                intent_summary TEXT NOT NULL,
                sql_used TEXT NOT NULL,
                result_summary TEXT NOT NULL,
                confidence REAL NOT NULL,
                found_answer TEXT NOT NULL CHECK(found_answer IN ('Да','Нет')),
                comment TEXT
            );
        """)  # [web:2]
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dialog_message (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                dialog_id INTEGER,
                turn_index INTEGER NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('system','user','assistant','tool')),
                content TEXT NOT NULL,
                meta TEXT,
                FOREIGN KEY (dialog_id) REFERENCES dialog_log(id) ON DELETE CASCADE
            );
        """)  # [web:2][web:48]
        self.conn.execute("CREATE INDEX IF NOT EXISTS ix_dialog_message_dialog_id ON dialog_message(dialog_id);")  # [web:2]
        self.conn.execute("CREATE INDEX IF NOT EXISTS ix_dialog_message_turn ON dialog_message(turn_index);")  # [web:2]
        self.conn.commit()

    @staticmethod
    def _now() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")  # [web:2]

    def log_message(self, turn_index: int, role: str, content: str, meta: Optional[Dict[str, Any]] = None, dialog_id: Optional[int] = None):
        assert self.conn is not None
        meta_json = json.dumps(meta, ensure_ascii=False) if meta is not None else None
        self.conn.execute("""
            INSERT INTO dialog_message (timestamp, dialog_id, turn_index, role, content, meta)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (self._now(), dialog_id, turn_index, role, content, meta_json))  # [web:2]
        self.conn.commit()  # [web:2]

    def log_final_answer(self, nl_query: str, answer: Dict[str, Any]) -> int:
        assert self.conn is not None
        conf = float(answer.get("confidence", 0.0))
        found = "Да" if conf >= 0.5 else "Нет"
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO dialog_log
                (timestamp, nl_query, intent_summary, sql_used, result_summary, confidence, found_answer, comment)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (self._now(), nl_query, answer.get("intent_summary",""), answer.get("sql_used",""),
              answer.get("result_summary",""), conf, found, ""))  # [web:2]
        self.conn.commit()
        return cur.lastrowid  # [web:2]

    def backfill_dialog_id(self, dialog_id: int):
        assert self.conn is not None
        self.conn.execute("UPDATE dialog_message SET dialog_id = ? WHERE dialog_id IS NULL", (dialog_id,))  # [web:2]
        self.conn.commit()  # [web:2]
        
    def reserve_dialog(self, nl_query: str) -> int:
        """
        Создаёт плейсхолдер в dialog_log и возвращает dialog_id.
        Все NOT NULL поля заполняются техническими значениями.
        """
        assert self.conn is not None
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO dialog_log
                (timestamp, nl_query, intent_summary, sql_used, result_summary, confidence, found_answer, comment)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            time.strftime("%Y-%m-%d %H:%M:%S"),  # timestamp
            nl_query,                           # nl_query
            "DIRECT PLACEHOLDER",               # intent_summary
            "DIRECT_PENDING",                   # sql_used
            "-",                                # result_summary
            0.0,                                # confidence
            "Нет",                              # found_answer
            ""                                   # comment
        ))
        self.conn.commit()                      # гарантируем фиксацию [web:2]
        return cur.lastrowid                    # безопасно для текущего соединения [web:49]

    def update_dialog(self, dialog_id: int, answer: Dict[str, Any]) -> None:
        """
        Финализирует строку в dialog_log с готовыми полями (без вставки новой строки).
        """
        assert self.conn is not None
        conf = float(answer.get("confidence", 0.0))
        found = "Да" if conf >= 0.5 else "Нет"
        self.conn.execute("""
            UPDATE dialog_log
               SET intent_summary = ?,
                   sql_used       = ?,
                   result_summary = ?,
                   confidence     = ?,
                   found_answer   = ?
             WHERE id = ?
        """, (
            answer.get("intent_summary", ""),
            answer.get("sql_used", ""),
            answer.get("result_summary", ""),
            conf,
            found,
            dialog_id
        ))
        self.conn.commit()                      # фиксируем UPDATE [web:2]