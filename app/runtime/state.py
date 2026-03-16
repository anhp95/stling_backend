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

    def to_dict(self) -> Dict[str, Any]:
        return {
            "wordlist": self.wordlist,
            "latest_data": self.latest_data,
            "last_output": self.last_output,
            "latest_data_source": self.latest_data_source,
            "latest_data_rows": self.latest_data_rows,
            "matrix_languages": self.matrix_languages,
            "matrix_concepts": self.matrix_concepts,
            "active_layers": self.active_layers,
            "history": self.history,
            "data_updated_this_turn": self.data_updated_this_turn,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConversationState":
        inst = cls()
        inst.wordlist = data.get("wordlist")
        inst.latest_data = data.get("latest_data")
        inst.last_output = data.get("last_output")
        inst.latest_data_source = data.get("latest_data_source")
        inst.latest_data_rows = data.get("latest_data_rows", 0)
        inst.matrix_languages = data.get("matrix_languages", 0)
        inst.matrix_concepts = data.get("matrix_concepts", 0)
        inst.active_layers = data.get("active_layers", {})
        inst.history = data.get("history", [])
        inst.data_updated_this_turn = data.get("data_updated_this_turn", False)
        return inst
