"""
Tool Registry — single source of truth for all tools.
"""

from typing import Dict, Any, List

from app.tools.schemas import ToolSpec

# ---- spoken language tools ----
from app.tools.impl.spoken_language.wordlist import (
    propose_wordlist,
)
from app.tools.impl.spoken_language.harvester import (
    collect_multilingual_rows,
)
from app.tools.impl.spoken_language.analysis import (
    read_csv,
    normalize_spoken_language_csv,
)
from app.domain.spoken_language.matrix import (
    to_binary_matrix,
)
from app.domain.spoken_language.clustering import (
    cluster,
)

# ---- viz tools ----
from app.tools.impl.viz.renderer import style_patch

# ---- data tools ----
from app.tools.impl.data.query import (
    query_spatial_linguistic_data,
    layer_query_plan,
    search_available_concepts,
)


_REGISTRY: Dict[str, Dict[str, Any]] = {
    "propose_wordlist": {
        "fn": propose_wordlist,
        "spec": ToolSpec(
            "propose_wordlist",
            "Generate a concept wordlist / gloss list by searching the internet/LLM knowledge.",
            is_async=True,
            needs_llm=True,
            agent="spoken_language",
        ),
    },
    "collect_multilingual_rows": {
        "fn": collect_multilingual_rows,
        "spec": ToolSpec(
            "collect_multilingual_rows",
            "Harvest multilingual data",
            is_async=True,
            needs_llm=True,
            agent="spoken_language",
        ),
    },
    "read_csv": {
        "fn": read_csv,
        "spec": ToolSpec(
            "read_csv",
            "Parse CSV structure",
            agent="spoken_language",
        ),
    },
    "normalize_spoken_language_csv": {
        "fn": normalize_spoken_language_csv,
        "spec": ToolSpec(
            "normalize_spoken_language_csv",
            "Repair and normalize CSV into standard format",
            agent="spoken_language",
        ),
    },
    "to_binary_matrix": {
        "fn": to_binary_matrix,
        "spec": ToolSpec(
            "to_binary_matrix",
            "Convert to binary matrix",
            agent="spoken_language",
        ),
    },
    "cluster": {
        "fn": cluster,
        "spec": ToolSpec(
            "cluster",
            "Cluster languages via HDBSCAN",
            agent="spoken_language",
        ),
    },
    "query_spatial_linguistic_data": {
        "fn": query_spatial_linguistic_data,
        "spec": ToolSpec(
            "query_spatial_linguistic_data",
            "Spatially-aware search over lexical, language, and other linguistic datasets in our internal / system / CLDF database.",
            agent="data_query",
        ),
    },
    "layer_query_plan": {
        "fn": layer_query_plan,
        "spec": ToolSpec(
            "layer_query_plan",
            "Build DuckDB query plan",
            is_async=True,
            needs_llm=True,
            agent="data_query",
        ),
    },
    "search_available_concepts": {
        "fn": search_available_concepts,
        "spec": ToolSpec(
            "search_available_concepts",
            "Search the internal CLDF database for available concepts / gloss list.",
            agent="data_query",
        ),
    },
    "style_patch": {
        "fn": style_patch,
        "spec": ToolSpec(
            "style_patch",
            "Modify visual properties of a layer",
            agent="viz",
        ),
    },
}


def list_tools() -> List[ToolSpec]:
    """Return specs for all registered tools."""
    return [v["spec"] for v in _REGISTRY.values()]


def get_tool(name: str) -> Dict[str, Any]:
    """Look up a tool entry by name."""
    if name not in _REGISTRY:
        raise KeyError(f"Unknown tool: {name}")
    return _REGISTRY[name]
