"""
Observations — Step and ExecutionTrace for debugging.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import time


@dataclass
class Step:
    """A single step in the pipeline execution."""

    stage: str  # "planner", "executor", "synthesizer"
    input_summary: str = ""
    output_summary: str = ""
    duration_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class ExecutionTrace:
    """Full trace of a single turn."""

    steps: List[Step] = field(default_factory=list)
    total_ms: float = 0.0

    def add_step(self, step: Step):
        self.steps.append(step)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "steps": [
                {
                    "stage": s.stage,
                    "duration_ms": s.duration_ms,
                    "error": s.error,
                }
                for s in self.steps
            ],
            "total_ms": self.total_ms,
        }


class StepTimer:
    """Context manager for timing a step."""

    def __init__(self, stage: str):
        self.step = Step(stage=stage)
        self._start = 0.0

    def __enter__(self):
        self._start = time.time()
        return self.step

    def __exit__(self, *args):
        elapsed = (time.time() - self._start) * 1000
        self.step.duration_ms = round(elapsed, 1)
