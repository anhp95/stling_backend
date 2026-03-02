"""Harvester tool — wraps domain with LLM call."""

from typing import Dict, List, Optional, Any
from app.domain.spoken_language.harvest import (
    build_harvest_prompt,
)


async def collect_multilingual_rows(
    wordlist: List[str] = None,
    scope: Optional[Dict] = None,
    llm_call_fn=None,
    **kwargs,
) -> Dict[str, Any]:
    """Collect multilingual data via LLM."""
    wordlist = wordlist or []
    # Handle flattened scope params
    final_scope = scope or {}
    if not final_scope:
        for k in [
            "language_families",
            "regions",
            "max_languages",
        ]:
            if k in kwargs:
                final_scope[k] = kwargs[k]
    try:
        prompt = build_harvest_prompt(wordlist, final_scope)
        if not llm_call_fn:
            return {
                "prompt": prompt,
                "wordlist": wordlist,
                "notes": "No LLM — prompt only",
            }
        csv_result = await llm_call_fn(prompt)
        return {
            "csv_data": csv_result,
            "prompt": prompt,
            "wordlist": wordlist,
            "notes": (f"Collected data for " f"{len(wordlist)} concepts"),
        }
    except Exception as e:
        return {
            "error": str(e),
        }
