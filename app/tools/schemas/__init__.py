"""Tool schemas."""

from dataclasses import dataclass


@dataclass
class ToolSpec:
    """Describes a registered tool."""

    name: str
    description: str
    is_async: bool = False
    needs_llm: bool = False
    agent: str = "shared"
