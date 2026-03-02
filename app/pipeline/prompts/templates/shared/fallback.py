"""Error + retry replanning prompt."""

FALLBACK_PROMPT = """\
The previous tool call failed with the following error:
{error}

Please either:
1. Suggest a corrective action using a different tool
2. Ask the user for clarification
3. Explain what went wrong

Respond with either a tool call JSON or plain text.
"""
