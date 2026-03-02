"""Viz renderer — style patch passthrough."""

from typing import Dict, Any, List


def style_patch(
    layername: str = "",
    column_names: List[str] = None,
    field: str = None,
    palette: str = None,
    patch: Dict = None,
    **kwargs,
) -> Dict[str, Any]:
    """Pass-through with validation: returns the style patch for frontend."""
    if patch is None:
        patch = {}

    error = None

    if field:
        if column_names is not None and field not in column_names:
            error = f"Field '{field}' does not exist in the provided columns {column_names}."
        else:
            patch["vizField"] = field

    if palette:
        patch["palette"] = palette

    if error:
        return {"error": error}

    return {
        "type": "style_patch",
        "layername": layername,
        "patch": patch,
    }
