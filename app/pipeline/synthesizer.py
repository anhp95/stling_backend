"""
Synthesizer — second LLM call that converts a
structured tool result into a human-friendly response.
"""

from app.shared.types import ToolResult, LLMCallFn
from app.pipeline.prompts import load_synthesizer_prompt
from app.tools.registry import get_tool


async def synthesize(
    tool_result: ToolResult,
    user_message: str,
    llm_call_fn: LLMCallFn,
) -> str:
    """Generate a human-friendly response."""
    if not tool_result.success:
        return (
            f"❌ **Tool error** "
            f"(`{tool_result.tool_name}`): "
            f"{tool_result.error}"
        )

    # Determine agent
    try:
        entry = get_tool(tool_result.tool_name)
        agent = entry["spec"].agent
    except KeyError:
        agent = "shared"

    prompt = load_synthesizer_prompt(
        agent=agent,
        tool_name=tool_result.tool_name,
        tool_result=tool_result.data,
        user_message=user_message,
    )

    from app.domain.spoken_language.csv_utils import strip_tool_calls

    try:
        response = await llm_call_fn([{"role": "user", "content": prompt}])
        cleaned = strip_tool_calls(response)
        return cleaned if cleaned else _fallback(tool_result)
    except Exception:
        return _fallback(tool_result)


def _fallback(tr: ToolResult) -> str:
    """Deterministic fallback."""
    d = tr.data
    name = tr.tool_name

    if "wordlist" in d and d["wordlist"]:
        n = len(d["wordlist"])
        preview = ", ".join(d["wordlist"][:8])
        sfx = "..." if n > 8 else ""
        return f"✅ **Wordlist** ({n} concepts): " f"{preview}{sfx}"
    if "csv_data" in d and d["csv_data"]:
        rows = d["csv_data"].count("\n")
        return f"✅ **{name}** — {rows} rows."
    if "geojson" in d and d["geojson"]:
        pts = d.get("point_count", "?")
        return f"✅ **Map layer** — {pts} points."
    if "glosses" in d and d["glosses"]:
        n = len(d["glosses"])
        preview = ", ".join(d["glosses"][:5])
        sfx = "..." if n > 5 else ""
        return f"✅ **Internal Search** ({n} items): {preview}{sfx}"
    if "type" in d:
        return f"✅ **{name}** completed."
    return f"✅ Tool `{name}` executed."
