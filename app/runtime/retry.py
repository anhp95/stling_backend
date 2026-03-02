"""
Retry — replanning loop with backoff and fallback.
"""

from typing import List, Dict

from app.shared.types import ToolResult, LLMCallFn
from app.pipeline.planner import plan, PlannerResult
from app.pipeline.prompts import load_fallback_prompt


async def replan_on_failure(
    tool_result: ToolResult,
    user_message: str,
    context_summary: str,
    history: List[Dict[str, str]],
    llm_call_fn: LLMCallFn,
) -> PlannerResult:
    """
    If a tool failed, ask the LLM to replan once.
    Returns a new PlannerResult (may be text-only).
    """
    fallback = load_fallback_prompt(tool_result.error or "Unknown error")
    augmented_msg = f"{user_message}\n\n" f"[System: {fallback}]"
    return await plan(
        message=augmented_msg,
        context_summary=context_summary,
        history=history,
        llm_call_fn=llm_call_fn,
    )
