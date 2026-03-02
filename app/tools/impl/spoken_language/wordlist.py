"""Wordlist tool — wraps domain with LLM call."""

from typing import Dict, Any, Optional
from app.domain.spoken_language.wordlist import (
    build_wordlist_prompt,
)
from app.domain.spoken_language.csv_utils import (
    extract_json_array,
)


async def propose_wordlist(
    topic: str,
    constraints: Optional[Dict] = None,
    llm_call_fn=None,
    **kwargs,
) -> Dict[str, Any]:
    """Generate a concept wordlist via LLM."""
    max_terms = 30
    if constraints and "max_terms" in constraints:
        max_terms = constraints["max_terms"]
    elif "max_terms" in kwargs:
        max_terms = kwargs["max_terms"]
    region = constraints.get("region") if constraints else kwargs.get("region")
    domain = constraints.get("domain") if constraints else kwargs.get("domain")
    prompt = build_wordlist_prompt(topic, max_terms, region, domain)
    if not llm_call_fn:
        return {
            "wordlist": [],
            "error": "No LLM function provided",
        }
    try:
        response = await llm_call_fn(prompt)
        if not response:
            return {
                "wordlist": [],
                "error": "Empty LLM response",
            }
        wordlist = extract_json_array(response)
        if wordlist is not None:
            return {
                "wordlist": wordlist,
                "notes": (f"Generated {len(wordlist)} " f"concepts for {topic}"),
            }
        return {
            "wordlist": [],
            "error": "No JSON list in response",
        }
    except Exception as e:
        return {
            "wordlist": [],
            "error": f"LLM call failed: {e}",
        }
