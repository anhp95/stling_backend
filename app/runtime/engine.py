"""
Engine — run_turn(state, user_msg) -> AgentResponse

The single entry point for the pipeline. Orchestrates:
  1. Planner (LLM) → decides tool or text
  2. Router + Executor → runs the tool
  3. Synthesizer (LLM) → formats the result
  4. Retry → replans on failure (once)
  5. Auto-Enrich → detects CSVs and coordinates for mapping/download
"""

from app.shared.types import AgentResponse, LLMCallFn
from app.runtime.state import ConversationState
from app.runtime.tracing import Trace
from app.runtime.observations import (
    ExecutionTrace,
    StepTimer,
)
from app.pipeline.planner import plan
from app.pipeline.executor import execute
from app.pipeline.synthesizer import synthesize
from app.runtime.retry import replan_on_failure


async def run_turn(
    state: ConversationState,
    user_message: str,
    llm_call_fn: LLMCallFn,
    frontend_context: str = None,
) -> AgentResponse:
    """
    Execute a single conversational turn.
    Returns AgentResponse with content + optional
    tool data.
    """
    trace = Trace()
    obs = ExecutionTrace()
    tool_result = None
    content = ""

    # ---- Step 1: Plan ----
    with StepTimer("planner") as step:
        span = trace.start_span("planner")

        summary = state.to_summary()
        if frontend_context:
            summary += f"\n\n[Frontend Active Context]\n{frontend_context}"

        planner_result = await plan(
            message=user_message,
            context_summary=summary,
            history=state.history,
            llm_call_fn=llm_call_fn,
        )
        trace.end_span(span)
        step.input_summary = user_message[:100]
        if planner_result.is_tool_call:
            tc = planner_result.tool_call
            step.output_summary = f"tool={tc.name}"
        else:
            step.output_summary = "text reply"
    obs.add_step(step)

    if not planner_result.is_tool_call:
        content = planner_result.text or ""
    else:
        # ---- Step 2: Execute ----
        with StepTimer("executor") as step:
            span = trace.start_span("executor")
            tool_result = await execute(
                tool_call=planner_result.tool_call,
                state=state,
                llm_call_fn=llm_call_fn,
                frontend_context=frontend_context,
            )
            trace.end_span(span)
            step.output_summary = f"success={tool_result.success}"
            if not tool_result.success:
                step.error = tool_result.error
        obs.add_step(step)

        # ---- Retry on failure ----
        if not tool_result.success:
            with StepTimer("retry") as step:
                span = trace.start_span("retry")
                retry_plan = await replan_on_failure(
                    tool_result=tool_result,
                    user_message=user_message,
                    context_summary=summary,
                    history=state.history,
                    llm_call_fn=llm_call_fn,
                )
                trace.end_span(span)
            obs.add_step(step)

            if retry_plan.is_tool_call:
                tool_result = await execute(
                    tool_call=retry_plan.tool_call,
                    state=state,
                    llm_call_fn=llm_call_fn,
                    frontend_context=frontend_context,
                )
            else:
                content = retry_plan.text or (f"❌ {tool_result.error}")

        # ---- Step 3: Synthesize ----
        if tool_result and not content:
            with StepTimer("synthesizer") as step:
                span = trace.start_span("synthesizer")
                content = await synthesize(
                    tool_result=tool_result,
                    user_message=user_message,
                    llm_call_fn=llm_call_fn,
                )
                trace.end_span(span)
                step.output_summary = content[:80]
            obs.add_step(step)

    # ---- Final Step: Auto-Enrich (Maps/Downloads) ----
    if not content:
        content = "Sorry, I didn't get a response. Please try again."
    res = AgentResponse(
        content=content,
        tool_name=tool_result.tool_name if tool_result else None,
        tool_data=tool_result.data if tool_result else {},
        trace_id=trace.trace_id,
    )
    _auto_enrich(res, state)

    state.data_updated_this_turn = False

    state.append_turn(user_message, content)
    obs.total_ms = trace.total_ms
    return res


def _auto_enrich(res: AgentResponse, state: ConversationState):
    """
    Pass latest_data to the frontend to automatically enable
    downloads and map layers.
    """
    if state.latest_data and getattr(state, "data_updated_this_turn", False):
        if not res.tool_data:
            res.tool_data = {}
        res.tool_data["latest_data"] = state.latest_data
        res.tool_data["can_download"] = True
