"""
Smoke test for the full layered architecture.
"""

import asyncio
import json
from app.runtime.state import ConversationState
from app.runtime.engine import run_turn


async def mock_llm(messages):
    """Mock LLM that returns tool calls for known prompts."""
    text = messages[-1]["content"]
    # Planner
    if any(
        "Available Tools" in m.get("content", "")
        for m in messages
        if m.get("role") == "system"
    ):
        if "kinship" in text.lower():
            return json.dumps(
                {
                    "tool": "propose_wordlist",
                    "params": {"topic": "kinship"},
                }
            )
        if "hello" in text.lower():
            return json.dumps(
                {
                    "text": "Hello! How can I help?",
                }
            )
    # propose_wordlist internal LLM call
    if "wordlist" in text.lower() or "kinship" in text.lower():
        return json.dumps(
            [
                "mother",
                "father",
                "sister",
                "brother",
                "aunt",
                "uncle",
                "cousin",
            ]
        )
    # Synthesizer
    if "Tool That Ran" in text:
        return "Done! Kinship wordlist with 7 concepts."
    return '{"text": "I do not understand."}'


async def test_tool_call():
    """Test: tool call pipeline."""
    state = ConversationState()
    result = await run_turn(state, "Create a kinship wordlist", mock_llm)
    assert result.content, "Empty response"
    assert result.tool_name == "propose_wordlist"
    assert state.wordlist is not None
    assert len(state.wordlist) == 7
    print(f"✅ Tool call: {result.tool_name}")
    print(f"   Wordlist: {state.wordlist}")
    print(f"   Response: {result.content[:80]}")
    print(f"   Trace: {result.trace_id}")


async def test_text_reply():
    """Test: conversational text."""
    state = ConversationState()
    result = await run_turn(state, "hello", mock_llm)
    assert "Hello" in result.content
    assert result.tool_name is None
    print(f"✅ Text reply: {result.content}")


async def test_unknown_tool():
    """Test: unknown tool triggers retry."""
    state = ConversationState()

    async def bad_llm(messages):
        return '{"tool": "nonexistent", "params": {}}'

    result = await run_turn(state, "do something", bad_llm)
    # Should get error content (retry also fails)
    assert result.content
    print(f"✅ Unknown tool: {result.content[:80]}")


async def test_context_enrichment():
    """Test: executor injects data from state."""
    state = ConversationState()
    state.wordlist = ["fire", "water", "earth"]

    # LLM planner returns collect_multilingual_rows
    # without wordlist param (should be auto-injected)
    call_count = {"n": 0}

    async def smart_llm(messages):
        call_count["n"] += 1
        text = messages[-1]["content"]
        if "Available Tools" in "".join(
            m.get("content", "") for m in messages if m.get("role") == "system"
        ):
            return json.dumps(
                {
                    "tool": "read_csv",
                    "params": {},
                }
            )
        if "Tool That Ran" in text:
            return "CSV has issues."
        return '{"text": "ok"}'

    state.raw_csv = "a,b\n1,2\n3,4"
    result = await run_turn(state, "show me the csv", smart_llm)
    assert result.tool_name == "read_csv"
    assert result.tool_data.get("row_count") == 2
    print(f"✅ Context enrichment: {result.tool_data}")


async def test_auto_map_enrichment():
    """Test: text response containing CSV with coords gets enriched."""
    state = ConversationState()

    async def csv_llm(messages):
        # Return a text response with a CSV block
        return json.dumps(
            {
                "text": "Here is some data:\n```csv\nLanguage Name,Latitude,Longitude\nEnglish,51.5,-0.12\nFrench,48.8,2.35\n```"
            }
        )

    result = await run_turn(state, "give me coords", csv_llm)
    assert result.tool_data.get("has_map") is True
    assert result.tool_data.get("can_download") is True
    assert "geojson" in result.tool_data
    assert result.tool_data["point_count"] == 2
    print(f"✅ Auto-enrichment: {result.tool_data['point_count']} points found in text")


async def test_tool_call_stripping():
    """Test: Accidental tool call JSON blocks are stripped from text."""
    state = ConversationState()

    async def messy_llm(messages):
        text = messages[-1]["content"]
        # If it's the synthesizer prompt
        if "Tool That Ran" in text:
            return (
                "Here is your wordlist! Enjoy.\n\n"
                '{"tool": "propose_wordlist", "params": {"topic": "kinship"}}\n'
                "I hope this helps your research."
            )

        # If it's the planner prompt
        if "Available Tools" in "".join(
            m["content"] for m in messages if m["role"] == "system"
        ):
            return '{"tool": "propose_wordlist", "params": {"topic": "kinship"}}'

        # Fallback for tool internal logic (wordlist generation)
        return json.dumps(["mother", "father"])

    result = await run_turn(state, "kinship words", messy_llm)
    assert '{"tool"' not in result.content
    assert "wordlist" in result.content
    assert "research" in result.content
    print(
        f"✅ Tool call stripping: Success (cleaned content: '{result.content[:30]}...')"
    )


async def test_source_clarification():
    """Test: Planner asks for clarification if source is missing for data search."""
    state = ConversationState()

    async def clarifying_llm(messages):
        text = messages[-1]["content"].lower()
        # Planner logic
        if (
            "available tools"
            in "".join(
                m.get("content", "") for m in messages if m.get("role") == "system"
            ).lower()
        ):
            if "internet" in text or "database" in text:
                # Source specified
                return '{"tool": "adhoc_spatial_query", "params": {"concepticon_glosses": ["kinship"]}}'
            else:
                # Source missing
                return '{"text": "Should I search the internet or internal CLDF database?"}'
        return '{"text": "synthesized reply"}'

    # Round 1: No source
    res1 = await run_turn(state, "Search for kinship data", clarifying_llm)
    assert "internet or internal" in res1.content
    assert res1.tool_name is None
    print(f"✅ Source clarification: Asked for source")

    # Round 2: Specify source
    res2 = await run_turn(state, "Use the internal database", clarifying_llm)
    assert res2.tool_name == "adhoc_spatial_query"
    print(f"✅ Source clarification: Processed with tool after specification")


async def test_internal_data_search():
    """Test: Flow for internal database search and data retrieval."""
    state = ConversationState()

    async def mock_internal_llm(messages):
        text = messages[-1]["content"].lower()
        system = "".join(
            m.get("content", "") for m in messages if m.get("role") == "system"
        ).lower()

        # Planner Phase
        if "available tools" in system:
            if "search" in text and "internal" not in text and "internet" not in text:
                return '{"text": "Search internet or internal database?"}'
            if "internal" in text:
                if "kinship" in text:
                    return '{"tool": "adhoc_spatial_query", "params": {"concepticon_glosses": ["kinship"]}}'
                return '{"tool": "search_internal_catalog", "params": {"query": "kin"}}'

        # Synthesizer Phase
        return "Here is your data from our internal CLDF records."

    # Round 1: Search without source -> Clarification
    res1 = await run_turn(state, "Search for kinship", mock_internal_llm)
    assert "internet or internal" in res1.content

    # Round 2: Specify internal -> Fetch data
    # Note: This test might fail if the parquet files don't actually exist on disk,
    # but the tool has error handling and returns a message in results.
    res2 = await run_turn(
        state, "Search the internal database for kinship", mock_internal_llm
    )
    assert res2.tool_name == "adhoc_spatial_query"
    # Even if file is missing, we check that it TRIED to call the tool correctly.
    print(f"✅ Internal search flow: Tool called successfully")


async def main():
    print("=" * 50)
    print("Layered Architecture Smoke Tests")
    print("=" * 50)
    print()
    await test_tool_call()
    print()
    await test_text_reply()
    print()
    await test_unknown_tool()
    print()
    await test_context_enrichment()
    print()
    await test_auto_map_enrichment()
    print()
    await test_tool_call_stripping()
    print()
    await test_source_clarification()
    print()
    await test_internal_data_search()
    print()
    print("All tests passed! ✅")


if __name__ == "__main__":
    asyncio.run(main())
