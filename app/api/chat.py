"""
Chat API — Conversation Controller.

Thin layer: receives HTTP, creates LLM call function,
calls engine.run_turn(), returns response.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import httpx
import time

from app.runtime.state import ConversationState
from app.runtime.engine import run_turn
from app.infra.settings import OLLAMA_BASE_URL

router = APIRouter()

# ---- session store ----
_sessions: Dict[str, ConversationState] = {}


def get_state(sid: str) -> ConversationState:
    if sid not in _sessions:
        _sessions[sid] = ConversationState()
    return _sessions[sid]


# ---- models ----


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: List[ChatMessage]
    model: str
    provider: str = "ollama"
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    context: Optional[str] = None
    session_id: str = "default"
    uploaded_file: Optional[str] = None


class ChatResponse(BaseModel):
    role: str
    content: str
    tool_name: Optional[str] = None
    tool_data: Optional[Dict[str, Any]] = None
    thinking_time: Optional[float] = None


# ---- LLM provider dispatch ----


async def call_llm(
    messages: List[Dict[str, str]],
    provider: str,
    model: str,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> str:
    """Call LLM with list of messages."""
    timeout = httpx.Timeout(120.0)
    async with httpx.AsyncClient(timeout=timeout) as c:
        if provider == "ollama":
            url = base_url or OLLAMA_BASE_URL
            hdrs = {"Content-Type": "application/json"}
            if api_key:
                hdrs["Authorization"] = f"Bearer {api_key}"
            r = await c.post(
                f"{url}/api/chat",
                headers=hdrs,
                json={
                    "model": model,
                    "messages": messages,
                    "stream": False,
                },
            )
            if r.status_code == 200:
                return r.json().get("message", {}).get("content", "")
            raise Exception(f"LLM error: {r.text}")

        elif provider == "openai":
            if not api_key:
                raise Exception("OpenAI key required")
            r = await c.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": messages,
                },
            )
            if r.status_code == 200:
                return r.json()["choices"][0]["message"]["content"]
            raise Exception(f"OpenAI ({r.status_code}): {r.text}")

        elif provider == "anthropic":
            if not api_key:
                raise Exception("Anthropic key required")
            sys_msg = next(
                (m["content"] for m in messages if m["role"] == "system"),
                "",
            )
            non_sys = [m for m in messages if m["role"] != "system"]
            r = await c.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": non_sys,
                    "system": sys_msg,
                    "max_tokens": 1024,
                },
            )
            if r.status_code == 200:
                return r.json()["content"][0]["text"]
            raise Exception(f"Anthropic ({r.status_code}): {r.text}")

        elif provider == "gemini":
            if not api_key:
                raise Exception("Gemini key required")
            contents = []
            for m in messages:
                role = "user" if m["role"] in ["user", "system"] else "model"
                contents.append(
                    {
                        "role": role,
                        "parts": [{"text": m["content"]}],
                    }
                )
            url = (
                "https://generativelanguage.googleapis"
                ".com/v1beta/models/"
                f"{model}:generateContent"
                f"?key={api_key}"
            )
            r = await c.post(
                url,
                headers={"Content-Type": "application/json"},
                json={"contents": contents},
            )
            if r.status_code == 200:
                return r.json()["candidates"][0]["content"]["parts"][0]["text"]
            raise Exception(f"Gemini ({r.status_code}): {r.text}")
        else:
            raise Exception(f"Unsupported provider: {provider}")


# ---- chat endpoint ----


@router.post("/chat", response_model=ChatResponse)
async def chat_with_llm(request: ChatRequest):
    """
    Pipeline chat endpoint.
    Delegates entirely to runtime.engine.run_turn().
    """
    t0 = time.time()
    state = get_state(request.session_id)

    # Handle file upload
    if request.uploaded_file:
        state.latest_data = request.uploaded_file
        state.latest_data_source = "upload"
        state.latest_data_rows = request.uploaded_file.count("\n")
        state.data_updated_this_turn = True

    user_msg = request.messages[-1].content if request.messages else ""

    async def llm_fn(msgs):
        return await call_llm(
            msgs,
            request.provider,
            request.model,
            request.api_key,
            request.base_url,
        )

    result = await run_turn(
        state=state,
        user_message=user_msg,
        llm_call_fn=llm_fn,
        frontend_context=request.context,
    )
    print("state", state.to_summary())

    return ChatResponse(
        role="assistant",
        content=result.content,
        tool_name=result.tool_name,
        tool_data=result.tool_data,
        thinking_time=round(time.time() - t0, 2),
    )


# ---- models endpoint ----


@router.get("/models")
async def list_models(
    provider: str = "ollama",
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
):
    """List available models."""
    try:
        timeout = httpx.Timeout(10.0)
        async with httpx.AsyncClient(timeout=timeout) as c:
            if provider == "ollama":
                url = base_url or OLLAMA_BASE_URL
                hdrs = {}
                if api_key:
                    hdrs["Authorization"] = f"Bearer {api_key}"
                r = await c.get(f"{url}/api/tags", headers=hdrs)
                if r.status_code == 200:
                    data = r.json()
                    models = []
                    for m in data.get("models", []):
                        try:
                            n = m.get("name", "")
                            if n:
                                models.append(str(n))
                        except (
                            UnicodeEncodeError,
                            UnicodeDecodeError,
                        ):
                            continue
                    return {"models": models}
                raise HTTPException(
                    status_code=r.status_code,
                    detail=f"Failed: {r.text}",
                )

            elif provider == "openai":
                if not api_key:
                    raise HTTPException(
                        status_code=400,
                        detail="API key required",
                    )
                r = await c.get(
                    "https://api.openai.com/v1/models",
                    headers={"Authorization": (f"Bearer {api_key}")},
                )
                if r.status_code == 200:
                    data = r.json()
                    ms = [m["id"] for m in data.get("data", [])]
                    chat = [m for m in ms if "gpt" in m.lower()]
                    return {"models": sorted(chat)}
                raise HTTPException(
                    status_code=r.status_code,
                    detail=f"Failed: {r.text}",
                )

            elif provider == "anthropic":
                if not api_key:
                    raise HTTPException(
                        status_code=400,
                        detail="API key required",
                    )
                r = await c.get(
                    "https://api.anthropic.com/v1/models",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                    },
                )
                if r.status_code == 200:
                    data = r.json()
                    ms = [m["id"] for m in data.get("data", [])]
                    return {"models": sorted(ms)}
                return {
                    "models": [
                        "claude-sonnet-4-20250514",
                        "claude-3-5-sonnet-20241022",
                        "claude-3-5-haiku-20241022",
                        "claude-3-opus-20240229",
                    ]
                }

            elif provider == "gemini":
                if not api_key:
                    raise HTTPException(
                        status_code=400,
                        detail="API key required",
                    )
                url = (
                    "https://generativelanguage"
                    ".googleapis.com/v1beta/models"
                    f"?key={api_key}"
                )
                r = await c.get(url)
                if r.status_code == 200:
                    data = r.json()
                    ms = []
                    for m in data.get("models", []):
                        n = m.get("name", "").replace("models/", "")
                        if n and "gemini" in n.lower():
                            ms.append(n)
                    return {"models": sorted(ms)}
                raise HTTPException(
                    status_code=r.status_code,
                    detail=f"Failed: {r.text}",
                )
            else:
                return {"models": []}

    except httpx.ConnectError:
        raise HTTPException(
            status_code=503,
            detail=(f"Cannot connect to {provider}."),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
