"""Binary availability matrix — pure computation."""

import io
import pandas as pd
from typing import Dict, Any


def to_binary_matrix(
    csv_data: str,
    **kwargs,
) -> Dict[str, Any]:
    """Convert linguistic CSV to binary availability matrix."""
    if not csv_data or not csv_data.strip():
        return {"error": "Empty CSV data", "summary": {}}

    try:
        df = pd.read_csv(io.StringIO(csv_data.strip()))

        required = ["Concept", "Form"]
        missing = [c for c in required if c not in df.columns]
        if "Glottocode" not in df.columns and "LangID" not in df.columns:
            missing.append("Glottocode or LangID")

        if missing:
            return {
                "error": f"Missing required columns for matrix: {missing}",
                "summary": {},
            }

        df = df.dropna(subset=["Form"])
        df = df[df["Form"].astype(str).str.strip() != ""]
        df["has_form"] = 1

        META_CANDIDATES = [
            "Glottocode",
            "LangID",
            "ID",
            "Language Family",
            "Language Name",
            "Latitude",
            "Longitude",
            "Source",
            "Date_Documented",
            "Date_Documented_Attribute",
            "Communalect",
            "Communalect_Group",
            "Language",
        ]
        # FIX 1: exclude columns that are entirely NaN — they poison groupby
        meta = [c for c in META_CANDIDATES if c in df.columns and df[c].notna().any()]

        # FIX 2: dropna=False so partially-null meta cols don't silently drop rows
        grouped = (
            df.groupby(meta + ["Concept"], dropna=False)["has_form"].max().reset_index()
        )

        # Patch NaNs before pivot_table re-groups
        null_meta = [c for c in meta if grouped[c].isna().any()]
        for c in null_meta:
            grouped[c] = grouped[c].fillna("__NA__")

        matrix = grouped.pivot_table(
            index=meta,
            columns="Concept",
            values="has_form",
            aggfunc="max",  # FIX 3: explicit, not accidental mean
            fill_value=0,
        ).reset_index()

        for c in null_meta:
            matrix[c] = matrix[c].replace("__NA__", pd.NA)

        concepts = [c for c in matrix.columns if c not in meta]
        matrix[concepts] = matrix[concepts].astype(int)

        out = matrix.to_csv(index=False)
        avg = float(round(matrix[concepts].to_numpy().mean() * 100, 1))

        return {
            "csv_data": out,
            "summary": {
                "languages": len(matrix),
                "concepts": len(concepts),
                "avg_coverage": avg,
            },
        }
    except Exception as e:
        return {"error": str(e), "summary": {}}
