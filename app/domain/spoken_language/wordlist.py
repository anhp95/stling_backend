"""Wordlist domain — pure data structures."""

from typing import List


def build_wordlist_prompt(
    topic: str,
    max_terms: int = 30,
    region: str = None,
    domain: str = None,
    **kwargs,
) -> str:
    """Build the LLM prompt for wordlist generation."""
    return (
        f"Generate a wordlist of {max_terms} concepts "
        f'for the semantic field: "{topic}"\n\n'
        f"Requirements:\n"
        f"- Culturally universal, semantically basic\n"
        f"- Well-documented across languages\n"
        f"- Distinct and clearly defined\n"
        f"- Suitable for cross-linguistic comparison\n\n"
        f"{f'Geographic focus: {region}' if region else ''}\n"
        f"{f'Domain focus: {domain}' if domain else ''}\n\n"
        f"Return ONLY a JSON array of strings:\n"
        f'["concept1", "concept2", ...]'
    )
