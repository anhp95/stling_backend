"""Viz planner prompt."""

PLANNER = """\
## Visualization Tools
- style_patch: Modify visual properties of a map layer.
  params: {{layername: str, column_names: list[str], field?: str, palette?: str, patch?: dict}}
  * If the user requests styling by a specific field, provide it in `field` and provide the layer's columns in `column_names`. The tool will validate it.
  * If the user wants a color palette, provide its name in `palette`.
  * If the user DOES NOT mention a color palette but styling by a field makes sense, you MUST suggest an appropriate default palette. 
    * For categorical/text data, use qualitative palettes: "Tableau10", "Set1", "Vivid", "Bold", "Safe".
    * For numerical data, use sequential/diverging palettes: "Viridis", "Plasma", "Turbo", "CoolWarm".
  * Common patch props: fillColor [r,g,b,a], radius, lineWeight, opacity, visible, labelEnabled (boolean), labelField (string).
"""
