"""
CSV utilities — pure validation, repair, parsing.
Zero LLM imports.
"""

import re
import json
from typing import Dict, List, Tuple, Any, Optional

# ---- LLM response helpers ----


def extract_json_array(text: str) -> Optional[List[str]]:
    """Extract a JSON array from text."""
    if not text:
        return None
    match = re.search(r"\[.*?\]", text, re.DOTALL)
    if match:
        try:
            result = json.loads(match.group(0))
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass
    return None


def strip_tool_calls(text: str) -> str:
    """
    Remove tool-call JSON blocks from LLM response text and unwrap
    plain-text reply envelopes like {"text": "..."}.
    """
    if not text:
        return ""

    # 0. Unwrap bare {"text": "..."} envelopes the planner sometimes emits
    #    verbatim instead of extracting the inner string.
    stripped_input = text.strip()
    if stripped_input.startswith("{"):
        try:
            obj = json.loads(stripped_input)
            if isinstance(obj, dict):
                # Pure text-reply envelope — return the inner string.
                if "text" in obj and not ("tool" in obj or "action" in obj):
                    inner = obj["text"] or ""
                    return strip_tool_calls(inner)  # recurse in case nested
        except json.JSONDecodeError:
            pass

    # 1. Remove code-fenced JSON blocks (tool/action blocks)
    cleaned = re.sub(
        r"```(?:json)?\s*\{[^`]*?(?:tool|action)[^`]*?\}\s*```",
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )

    # 2. Iteratively remove balanced JSON objects containing "tool" or "action"
    while True:
        start = -1
        for i in range(len(cleaned)):
            if cleaned[i] == "{":
                fragment = cleaned[i : i + 60].lower()
                if "tool" in fragment or "action" in fragment:
                    start = i
                    break

        if start == -1:
            break

        end = -1
        depth = 0
        in_str = False
        esc = False
        for j in range(start, len(cleaned)):
            c = cleaned[j]
            if esc:
                esc = False
                continue
            if c == "\\":
                esc = True
                continue
            if c == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    end = j
                    break

        if end != -1:
            obj_text = cleaned[start : end + 1]
            if "tool" in obj_text.lower() or "action" in obj_text.lower():
                cleaned = cleaned[:start] + cleaned[end + 1 :]
                continue

        # Not a tool call — skip this brace to avoid infinite loop
        cleaned = cleaned[:start] + "\u200c" + cleaned[start + 1 :]

    cleaned = cleaned.replace("\u200c", "{")
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()
