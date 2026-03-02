"""Harvest domain — prompt template for data collection."""

from typing import Dict, List, Optional


def build_harvest_prompt(
    wordlist: List[str],
    scope: Optional[Dict] = None,
) -> str:
    """Build the LLM prompt for multilingual harvest."""
    items = "\n".join(f"- {w}" for w in wordlist)
    scope_text = ""
    if scope:
        fams = scope.get("language_families")
        if fams:
            scope_text += f"\nFocus on families: {', '.join(fams)}"
        regs = scope.get("regions")
        if regs:
            scope_text += f"\nFocus on regions: {', '.join(regs)}"
        mx = scope.get("max_languages")
        if mx:
            scope_text += f"\nLimit to ~{mx} languages"
    return f"""Task: Collect multilingual linguistic data for the exact concepts listed below:
{items}

### Linguistic Search & Coverage Rules (Generalized for Any Wordlist)

For each target **word or concept** in the user-provided wordlist, the primary objective is to identify **corresponding lexical forms** expressing the same concept in **as many languages as possible**.

1. **Lexical Form Discovery (Primary Objective)**
   - Actively search for **attested lexical forms** that correspond to the target word or concept, including:
     - Cognates
     - Inherited forms
     - Loanwords
     - Calques
     - Closely related or semantically equivalent lexical items
   - The focus is on **lexical realization**, not orthographic similarity alone.
   - Include culturally specific variants that encode the same concept, even when the surface form differs substantially.

2. **Priority Scope**
   - Identify language families and geographic regions where the target concept is:
     - Historically attested
     - Culturally significant
     - Frequently documented in linguistic or ethnographic literature
   - Prioritize these regions and families to maximize coverage of relevant lexical forms.

3. **Global Expansion**
   - After covering priority regions, expand the search to **all other languages with reliable documentation**, aiming for **maximal cross-linguistic coverage**.
   - Include both historical and contemporary lexical data when available.

4. **Per-Row Information Requirement (Force-fill with Strict Geospatial Validation)**
   - For every **(Language × Concept × Lexical Form)**, you MUST attempt to retrieve:
     - Glottocode (from Glottolog)
     - Language Family (Glottolog classification)
     - Standardized Language Name (Glottolog)
     - Concept (from the wordlist)
     - **Form (the attested lexical form expressing the concept)**
     - **Latitude and Longitude (see rules below)**
     - Source (dictionary, grammar, database, or ethnographic reference)

   - **Coordinate Rules (CRITICAL):**
     - Latitude and Longitude MUST be provided for every row.
     - Coordinates MUST correspond to a **real, mappable geographic location** and be valid for map visualization.
     - **Primary source:** Glottolog language-level coordinates.
     - **Fallback:** if language-level coordinates are unavailable, use a **standardized country-level reference point** for the primary country where the language is spoken.
       - The country reference must come from an authoritative dataset (e.g., ISO country centroids, Natural Earth, or equivalent).
       - The same country must always resolve to the same coordinates.
     - **Do NOT**:
       - Generate random coordinates
       - Use arbitrary offsets or noise
       - Guess from vague regional descriptions
       - Use placeholder or dummy values (e.g., `0`, `1`, `999`)
     - Coordinates must satisfy:
       - Latitude ∈ [-90, 90]
       - Longitude ∈ [-180, 180]
       - Numeric and finite values only

   - Do not output a row unless a **Source** is available.
   - Never guess or invent linguistic data.
   - Geographic estimation is allowed **only** under the controlled fallback rules above.

5. **Output Format (STRICT CSV)**
   - Output **only CSV**, UTF-8 encoded.
   - Columns must appear in this exact order:
     ```
     Glottocode,Language Family,Language Name,Concept,Form,Latitude,Longitude,Source
     ```
   - One row per (Language × Concept × Lexical Form).
   - Any field containing commas, quotes, or newlines (especially `Language Name` and `Source`) MUST be wrapped in double quotes.
   - Internal double quotes must be escaped as `""`.
   - Start with the header row, then data rows.
   - Do not include explanations, markdown, or extra text.

**Goal:** produce a **maximally comprehensive, geographically valid, and schema-strict cross-linguistic inventory of corresponding lexical forms**, suitable for direct computational analysis and accurate map visualization.
"""
