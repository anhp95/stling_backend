"""
PromptLoader — load(agent, stage) -> str

Abstracts prompt format (Python template strings).
Composes agent-specific prompts with shared preamble.
"""

import json
from typing import Dict, Any

from app.pipeline.prompts.templates.shared.system import (
    SYSTEM_PERSONA,
)
from app.pipeline.prompts.templates.shared.fallback import (
    FALLBACK_PROMPT,
)

# Agent planner fragments
from app.pipeline.prompts.templates.spoken_language.planner import (
    PLANNER as SL_PLANNER,
)
from app.pipeline.prompts.templates.data_query.planner import (
    PLANNER as DQ_PLANNER,
)
from app.pipeline.prompts.templates.viz.planner import (
    PLANNER as VIZ_PLANNER,
)

# Agent synthesizer fragments
from app.pipeline.prompts.templates.spoken_language.synthesizer import (
    SYNTHESIZER as SL_SYNTH,
)
from app.pipeline.prompts.templates.data_query.synthesizer import (
    SYNTHESIZER as DQ_SYNTH,
)
from app.pipeline.prompts.templates.viz.synthesizer import (
    SYNTHESIZER as VIZ_SYNTH,
)


_PLANNER_MAP = {
    "spoken_language": SL_PLANNER,
    "data_query": DQ_PLANNER,
    "viz": VIZ_PLANNER,
}

_SYNTH_MAP = {
    "spoken_language": SL_SYNTH,
    "data_query": DQ_SYNTH,
    "viz": VIZ_SYNTH,
}


def load_planner_prompt(
    context_summary: str,
) -> str:
    """
    Build the full planner system prompt.
    Combines persona + ALL agent tool sections.
    """
    tool_sections = "\n".join(_PLANNER_MAP.values())

    return f"""{SYSTEM_PERSONA}

You are the Planner. Decide whether the user's message \
needs a tool call or a plain conversational reply.

## Session Data
{context_summary}

## Available Tools
{tool_sections}

## Instructions
- If the user wants to search for linguistic data but does not specify a source (e.g. "search for kinship data"), you MUST ask whether they want to search the **internet** (for harvesting) or the **internal CLDF database**. Do NOT call a tool until the source is specified.
- If the source is **internal CLDF database**, use `query_spatial_linguistic_data` or `search_available_concepts`.
- If the source is **internet**, use `collect_multilingual_rows`.
- If the user wants a tool, reply with ONLY JSON:
  {{"tool": "<tool_name>", "params": {{...}}}}
- If no tool is needed (including when asking for clarification), reply with ONLY:
  {{"text": "<your conversational reply>"}}
- Never mix tool calls and text.
- Omit params that can be inferred from session data \
(e.g. csv_data is auto-injected from context).
"""


def load_synthesizer_prompt(
    agent: str,
    tool_name: str,
    tool_result: Dict[str, Any],
    user_message: str,
) -> str:
    """
    Build the synthesizer prompt for a given agent.
    """
    agent_hint = _SYNTH_MAP.get(agent, "")
    result_str = json.dumps(tool_result, default=str)
    if len(result_str) > 3000:
        result_str = result_str[:3000] + "...(truncated)"

    return f"""{SYSTEM_PERSONA}

{agent_hint}

## User's Original Message
{user_message}

## Tool That Ran
{tool_name}

## Tool Result
{result_str}

Summarize the result in a friendly, informative way. \
Use markdown. Keep it concise. Do NOT output JSON.
"""


def load_fallback_prompt(error: str) -> str:
    """Load the retry/fallback prompt."""
    return FALLBACK_PROMPT.format(error=error)
