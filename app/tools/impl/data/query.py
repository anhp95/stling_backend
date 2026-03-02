"""Data query tool implementations."""

import json
import re
from typing import Dict, Any, List

from app.shared.types import LLMCallFn


from app.domain.catalog.service import fetch_internal_data_csv, search_glosses


def query_spatial_linguistic_data(
    concepticon_glosses: List[str] = None,
    dataset: str = "Combined",
    lat: float = None,
    lon: float = None,
    radius_km: float = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Fetch actual linguistic data for concepts from the internal database.
    Returns CSV data that can be mapped and downloaded.
    """
    if not concepticon_glosses:
        return {"error": "No concepts specified for search."}

    concepticon_glosses = [g.upper() for g in concepticon_glosses]

    csv_data = fetch_internal_data_csv(
        glosses=concepticon_glosses,
        dataset=dataset,
        lat=lat,
        lon=lon,
        radius_km=radius_km,
    )

    if csv_data.startswith("Error") or csv_data.startswith("No"):
        return {"error": csv_data}

    row_count = csv_data.strip().count("\n")
    return {
        "csv_data": csv_data,
        "row_count": row_count,
        "wordlist": concepticon_glosses,
        "dataset": dataset,
        "can_download": True,
        "has_map": True,
        "message": f"Fetched {row_count} rows for concepts: {', '.join(concepticon_glosses[:5])}",
    }


def search_available_concepts(
    query: str = "",
    **kwargs,
) -> Dict[str, Any]:
    """Search the internal CLDF database for available concepts/glosses."""
    items = search_glosses(query=query)

    if not items:
        return {
            "wordlist": [],
            "count": 0,
            "message": f"No concepts matching '{query}' found in the internal database.",
        }

    return {
        "wordlist": items,
        "count": len(items),
        "message": f"Found {len(items)} matching concepts in the internal database.",
    }


async def layer_query_plan(
    layername: str = "",
    user_request: str = "",
    is_spatial: bool = False,
    min_lat: float = None,
    max_lat: float = None,
    min_lon: float = None,
    max_lon: float = None,
    frontend_context: str = "",
    llm_call_fn: LLMCallFn = None,
    **kwargs,
) -> Dict[str, Any]:
    """Build a DuckDB query plan via LLM."""
    spatial_instructions = ""
    if (
        is_spatial
        and min_lat is not None
        and max_lat is not None
        and min_lon is not None
        and max_lon is not None
    ):
        spatial_instructions = (
            f"\nThe user intends to filter the data spatially based on their request. "
            f"\nCRITICAL: Apply a spatial bounding box filter with the following coordinates:\n"
            f"Latitude BETWEEN {min_lat} AND {max_lat} AND Longitude BETWEEN {min_lon} AND {max_lon}\n"
            f"You MUST use the exact coordinate column names from the context schema (e.g. Latitude/Longitude or lat/lon).\n"
            f"DO NOT make up location names or fabricate string matches for location columns."
        )

    prompt = (
        f"You are formulating a DuckDB WASM SQL query to filter "
        f"layer '{layername}'.\n"
        f"Active Layers and Columns (Frontend Context): {frontend_context}\n"
        f"User Request: {user_request}\n\n"
        f"Identify the appropriate columns to filter based on the schema and request.\n"
        f"{spatial_instructions}\n"
        f"Return ONLY a JSON object containing the raw SQL query string inside the 'query' key:\n"
        f'{{"query": "SELECT * FROM data WHERE ... "}}'
    )
    plan = {}
    if llm_call_fn:
        raw = await llm_call_fn([{"role": "user", "content": prompt}])
        plan = _parse_json(raw)
    return {
        "type": "query_plan",
        "layername": layername,
        "plan": plan,
    }


def _parse_json(text: str) -> Dict:
    code = re.search(
        r"```(?:json)?\s*(\{.*?\})\s*```",
        text,
        re.DOTALL,
    )
    if code:
        try:
            return json.loads(code.group(1))
        except json.JSONDecodeError:
            pass
    bare = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if bare:
        try:
            return json.loads(bare.group(0))
        except json.JSONDecodeError:
            pass
    return {}
