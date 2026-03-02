"""
Executor — runs a tool and updates state.
"""

from typing import Dict, Any

from app.shared.types import ToolCall, ToolResult, LLMCallFn
from app.tools.registry import get_tool
from app.runtime.state import ConversationState


async def execute(
    tool_call: ToolCall,
    state: ConversationState,
    llm_call_fn: LLMCallFn,
    frontend_context: str = None,
) -> ToolResult:
    """Execute a tool call and update state."""
    name = tool_call.name
    params = dict(tool_call.params)

    try:
        entry = get_tool(name)
        fn = entry["fn"]
        spec = entry["spec"]
    except KeyError as e:
        return ToolResult(
            tool_name=name,
            success=False,
            error=str(e),
        )

    params = _enrich(name, params, state)

    err = _validate(name, params, state)
    if err:
        return ToolResult(
            tool_name=name,
            success=False,
            error=err,
        )

    try:
        kwargs = dict(params)
        kwargs["frontend_context"] = frontend_context

        if spec.needs_llm:
            if name == "layer_query_plan":
                kwargs["llm_call_fn"] = llm_call_fn
            else:

                async def _str_llm(prompt: str) -> str:
                    return await llm_call_fn([{"role": "user", "content": prompt}])

                kwargs["llm_call_fn"] = _str_llm

        if spec.is_async:
            result = await fn(**kwargs)
        else:
            result = fn(**kwargs)

        _update_state(name, result, state)

        data = result if isinstance(result, dict) else {"raw": result}
        return ToolResult(
            tool_name=name,
            success=True,
            data=data,
        )

    except Exception as e:
        return ToolResult(
            tool_name=name,
            success=False,
            error=str(e) or e.__class__.__name__,
        )


def _enrich(
    name: str,
    params: Dict[str, Any],
    s: ConversationState,
) -> Dict[str, Any]:
    p = dict(params)

    if name == "collect_multilingual_rows":
        if "wordlist" not in p and s.wordlist:
            p["wordlist"] = s.wordlist

    csv_tools = [
        "read_csv",
        "normalize_spoken_language_csv",
        "to_binary_matrix",
        "cluster",
    ]
    if name in csv_tools:
        if "csv_data" not in p and s.latest_data:
            p["csv_data"] = s.latest_data

    return p


def _validate(
    name: str,
    params: Dict[str, Any],
    s: ConversationState,
) -> str:
    csv_tools = {
        "read_csv": "csv_data",
        "normalize_spoken_language_csv": "csv_data",
        "to_binary_matrix": "csv_data",
        "cluster": "csv_data",
    }
    if name in csv_tools:
        key = csv_tools[name]
        if key not in params or not params[key]:
            if not s.has_any_data():
                return "No data available."
    if name == "collect_multilingual_rows":
        wl = params.get("wordlist")
        if not wl and not s.wordlist:
            return "No wordlist available."
    return ""


def _update_state(
    name: str,
    result: Any,
    s: ConversationState,
):
    if not isinstance(result, dict):
        return

    if name in ("propose_wordlist", "search_available_concepts"):
        if "wordlist" in result:
            s.wordlist = result["wordlist"]

    if name == "collect_multilingual_rows":
        csv = result if isinstance(result, str) else result.get("csv_data")
        if csv:
            s.latest_data = csv
            s.latest_data_source = "harvest"
            s.latest_data_rows = csv.count("\n")
            s.data_updated_this_turn = True
        if "wordlist" in result:
            s.wordlist = result["wordlist"]

    if name == "normalize_spoken_language_csv" and "csv_data" in result:
        s.latest_data = result["csv_data"]
        s.latest_data_rows = result.get("row_count", result["csv_data"].count("\n"))
        s.data_updated_this_turn = True

    if name == "to_binary_matrix" and "csv_data" in result:
        s.latest_data = result["csv_data"]
        summary = result.get("summary", {})
        s.matrix_languages = summary.get("languages", 0)
        s.matrix_concepts = summary.get("concepts", 0)
        s.data_updated_this_turn = True

    if name == "cluster" and "csv_data" in result:
        s.latest_data = result["csv_data"]
        s.data_updated_this_turn = True

    if name == "query_spatial_linguistic_data" and "csv_data" in result:
        s.latest_data = result["csv_data"]
        s.latest_data_source = "query"
        s.latest_data_rows = result.get("row_count", result["csv_data"].count("\n"))
        s.data_updated_this_turn = True

    s.last_output = result
