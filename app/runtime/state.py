"""
ConversationState — session state for a conversation.
"""

from typing import Dict, List, Optional, Any


class ConversationState:
    """Mutable session state: data artifacts + history."""

    def __init__(self):
        # Linguistic data artifacts
        self.wordlist: Optional[List[str]] = None
        self.latest_data: Optional[str] = None
        self.last_output: Optional[Dict] = None

        # Metadata
        self.latest_data_source: Optional[str] = None
        self.latest_data_rows: int = 0
        self.matrix_languages: int = 0
        self.matrix_concepts: int = 0

        # Map layers for viz/query tools
        self.active_layers: Dict[str, Any] = {}

        # Conversation history
        self.history: List[Dict[str, str]] = []

        # Data update tracking
        self.data_updated_this_turn: bool = False

    def get_active_csv(self) -> Optional[str]:
        return self.latest_data

    def has_any_data(self) -> bool:
        return bool(self.latest_data)

    def to_summary(self) -> str:
        """One-liner for the planner prompt."""
        parts = []
        if self.wordlist:
            parts.append(f"wordlist ({len(self.wordlist)} concepts)")
        if self.latest_data:
            parts.append(f"latest_data ({self.latest_data_rows} rows)")
        return ", ".join(parts) if parts else "none"

    def append_turn(self, user: str, assistant: str):
        self.history.append({"role": "user", "content": user})
        self.history.append({"role": "assistant", "content": assistant})
