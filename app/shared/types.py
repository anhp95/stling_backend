"""Cross-cutting types not owned by any single layer."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
)


# Single definition of the LLM call signature
LLMCallFn = Callable[[List[Dict[str, str]]], Awaitable[str]]


@dataclass
class ToolCall:
    """Structured tool invocation from the planner."""

    name: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResult:
    """Structured result of a tool execution."""

    tool_name: str
    success: bool
    data: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class AgentResponse:
    """Final response returned to the API layer."""

    content: str
    tool_name: Optional[str] = None
    tool_data: Optional[Dict[str, Any]] = None
    trace_id: Optional[str] = None
