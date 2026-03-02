"""
Planner — first LLM call in the pipeline.

Returns either:
  ToolCall(name, params)  — execute a tool
  or plain text           — conversational reply
"""

import json
import re
from typing import Dict, List, Optional

from app.shared.types import ToolCall, LLMCallFn
from app.pipeline.prompts import load_planner_prompt


class PlannerResult:
    """Outcome of the planner step."""

    def __init__(
        self,
        tool_call: Optional[ToolCall] = None,
        text: Optional[str] = None,
        raw_llm: str = "",
    ):
        self.tool_call = tool_call
        self.text = text
        self.raw_llm = raw_llm

    @property
    def is_tool_call(self) -> bool:
        return self.tool_call is not None


async def plan(
    message: str,
    context_summary: str,
    history: List[Dict[str, str]],
    llm_call_fn: LLMCallFn,
) -> PlannerResult:
    """Run the planner LLM call."""
    system = load_planner_prompt(context_summary)
    conversation: List[Dict[str, str]] = [
        {"role": "system", "content": system},
    ]
    conversation.extend(history[-10:])
    conversation.append({"role": "user", "content": message})
    try:
        raw = await llm_call_fn(conversation)
    except Exception as e:
        return PlannerResult(text=f"LLM error: {e}", raw_llm="")
    return _parse(raw)


def _parse(raw: str) -> PlannerResult:
    """Parse planner LLM output."""
    # 1. Code-fenced JSON
    code = re.search(
        r"```(?:json)?\s*(\{.*?\})\s*```",
        raw,
        re.DOTALL,
    )
    json_str = code.group(1) if code else None

    # 2. Full string as JSON
    if not json_str:
        s = raw.strip()
        if s.startswith("{"):
            json_str = s

    # 3. Balanced-brace extraction
    if not json_str:
        json_str = _extract_json(raw)

    from app.domain.spoken_language.csv_utils import strip_tool_calls

    if json_str:
        try:
            data = json.loads(json_str)
            # Tool call
            name = data.get("tool") or data.get("action")
            if name:
                return PlannerResult(
                    tool_call=ToolCall(
                        name=name,
                        params=data.get("params", {}),
                    ),
                    raw_llm=raw,
                )
            # Text reply
            if "text" in data:
                return PlannerResult(text=strip_tool_calls(data["text"]), raw_llm=raw)
        except json.JSONDecodeError:
            pass

    # 4. Fallback
    cleaned = strip_tool_calls(raw)
    if not cleaned:
        cleaned = "I'm not sure how to help with that. Could you rephrase?"
    return PlannerResult(text=cleaned, raw_llm=raw)


def _extract_json(text: str) -> Optional[str]:
    """Extract first balanced JSON object."""
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        c = text[i]
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
                return text[start : i + 1]
    return None
