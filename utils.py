# utils.py
import json
import re
from typing import Dict, List, Optional

def extract_json_object(text: str) -> Optional[Dict]:
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"``````", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except Exception:
            pass
    return None  # [web:2]

def build_incremental_payload(messages: List[Dict[str, str]], start_idx: int) -> str:
    pieces = []
    for m in messages[start_idx:]:
        if m["role"] == "assistant":
            continue
        pieces.append(f"## {m['role']}\n{m['content']}")
    return "\n---\n".join(pieces)  # [web:2]
