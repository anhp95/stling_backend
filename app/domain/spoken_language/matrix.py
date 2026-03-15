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

        meta = [
            "Glottocode",
            "LangID",
            "Language Family",
            "Language Name",
            "Latitude",
            "Longitude",
        ]
        meta = [c for c in meta if c in df.columns]

        grouped = df.groupby(meta + ["Concept"])["has_form"].first().reset_index()
        matrix = grouped.pivot_table(
            index=meta,
            columns="Concept",
            values="has_form",
            fill_value=0,
        ).reset_index()

        concepts = [c for c in matrix.columns if c not in meta]
        for col in concepts:
            matrix[col] = matrix[col].astype(int)

        out = matrix.to_csv(index=False)

        if concepts:
            avg = float(round(matrix[concepts].mean().mean() * 100, 1))
        else:
            avg = 0.0

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
