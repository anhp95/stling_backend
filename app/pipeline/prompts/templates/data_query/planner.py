"""Data query planner prompt."""

PLANNER = """\
## Data Query Tools
- query_spatial_linguistic_data: Spatially-aware search over lexical, language, and other linguistic datasets in our internal / system / CLDF database.
  params: {{concepticon_glosses: list[str], dataset?: str, lat?: float, lon?: float, radius_km?: float}}
  * Note: If the user specifies a location vaguely (place name, region, landmark, admin area, "near X", "around Y"), estimate an appropriate representative `lat` and `lon` along with a suitable `radius_km` bounding radius depending on location granularity (e.g., country/region -> large radius like 500-1000km, city/district -> medium like 50km, landmark -> small like 10km). ALWAYS include estimated coordinates and radius if a location is mentioned, even if explicit coordinates weren't provided.
- search_available_concepts: Search the internal CLDF database for available concepts / gloss list.
  params: {{query?: str}}
- layer_query_plan: Generate a DuckDB WASM SQL query to filter a layer. The tool automatically receives frontend context containing the layer's schema and query from user. Focus on matching fields and values.
  params: {{layername: str, user_request: str, is_spatial: bool, min_lat?: float, max_lat?: float, min_lon?: float, max_lon?: float}}
  * Note: If the user request involves geographic locations (e.g., 'in Europe', 'near New York'), YOU MUST set `is_spatial` to TRUE and estimate the geographic bounding box of the given location, providing `min_lat`, `max_lat`, `min_lon`, `max_lon`. DO NOT fabricate location string column matches. Use the estimated bounding box for `is_spatial` queries.
"""
