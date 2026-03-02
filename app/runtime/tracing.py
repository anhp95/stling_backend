"""
Tracing — spans and telemetry hooks.
"""

from dataclasses import dataclass, field
from typing import Dict, Any
import uuid
import time


@dataclass
class Span:
    """A named span for tracing."""

    name: str
    trace_id: str = ""
    start_time: float = 0.0
    end_time: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        return (self.end_time - self.start_time) * 1000


class Trace:
    """A collection of spans for a single request."""

    def __init__(self):
        self.trace_id = str(uuid.uuid4())[:8]
        self.spans: list = []

    def start_span(self, name: str) -> Span:
        span = Span(
            name=name,
            trace_id=self.trace_id,
            start_time=time.time(),
        )
        self.spans.append(span)
        return span

    def end_span(self, span: Span):
        span.end_time = time.time()

    @property
    def total_ms(self) -> float:
        if not self.spans:
            return 0.0
        s = min(sp.start_time for sp in self.spans)
        e = max(sp.end_time or time.time() for sp in self.spans)
        return (e - s) * 1000
