"""Analysis tool — parsing and normalisation."""

import io
import pandas as pd
from typing import Dict, Any


def read_csv(csv_data: str, **kwargs) -> Dict[str, Any]:
    """Parse CSV and return structure."""
    if not csv_data or not csv_data.strip():
        return {
            "ok": False,
            "columns": [],
            "row_count": 0,
            "preview": [],
            "error": "Empty CSV data",
        }
    try:
        df = pd.read_csv(io.StringIO(csv_data.strip()))
        return {
            "ok": True,
            "csv_data": csv_data,
            "columns": list(df.columns),
            "row_count": len(df),
            "preview": df.head(5).to_dict(orient="records"),
        }
    except Exception as e:
        return {
            "ok": False,
            "columns": [],
            "row_count": 0,
            "preview": [],
            "error": str(e),
        }


def normalize_spoken_language_csv(csv_data: str, **kwargs) -> Dict[str, Any]:
    """Repair and normalize CSV into standard format."""
    if not csv_data or not csv_data.strip():
        return {"ok": False, "error": "Empty CSV data"}
    try:
        df = pd.read_csv(io.StringIO(csv_data.strip()))

        # Trim whitespace from headers
        df.columns = df.columns.str.strip()

        # Auto-rename columns
        rename_map = {}
        for col in df.columns:
            low = col.lower()
            if "form" in low and not any(r == "Form" for r in rename_map.values()):
                rename_map[col] = "Form"
            elif (
                "concept" in low
                or "gloss" in low
                or "parameter" in low
                or "param" in low
            ) and not any(r == "Concept" for r in rename_map.values()):
                rename_map[col] = "Concept"
            elif ("glottocode" in low or "glotto" in low) and not any(
                r == "Glottocode" for r in rename_map.values()
            ):
                rename_map[col] = "Glottocode"
            elif ("langid" in low) and not any(
                r == "LangID" for r in rename_map.values()
            ):
                rename_map[col] = "LangID"
            elif ("lat" in low or low == "y") and not any(
                r == "Latitude" for r in rename_map.values()
            ):
                rename_map[col] = "Latitude"
            elif (
                "lon" in low or "lng" in low or "long" in low or low == "x"
            ) and not any(r == "Longitude" for r in rename_map.values()):
                rename_map[col] = "Longitude"

        df.rename(columns=rename_map, inplace=True)

        # Remove duplicated columns (keep first)
        df = df.loc[:, ~df.columns.duplicated()]

        # Check required columns
        required = [
            "Concept",
            "Form",
            "Latitude",
            "Longitude",
        ]
        missing = [c for c in required if c not in df.columns]

        if "Glottocode" not in df.columns and "LangID" not in df.columns:
            missing.append("Glottocode or LangID")

        if missing:
            return {
                "ok": False,
                "error": f"Normalization failed. Missing required columns: {', '.join(missing)}.\\nPlease fix the CSV and re-upload.",
                "missing": missing,
            }

        normalized_csv = df.to_csv(index=False)
        return {
            "ok": True,
            "csv_data": normalized_csv,
            "row_count": len(df),
            "warnings": [],
        }
    except Exception as e:
        return {"ok": False, "error": f"Failed to parse or normalize CSV: {e}"}
