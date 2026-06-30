"""
E2E integration test: FirestoreSaver with built-in MessageReducer.

Requires:
  - GOOGLE_APPLICATION_CREDENTIALS env var pointing to a service account JSON, or
    Application Default Credentials already configured (gcloud auth application-default login)
  - OPENAI_API_KEY env var
  - GCP_PROJECT_ID env var (defaults to 'gcdeveloper-new')
"""
import os
import pytest
from langgraph.graph import StateGraph, MessagesState, START
from langchain_openai import ChatOpenAI
from langgraph_checkpoint_firestore import FirestoreSaver
from agentstate_reducer import MessageReducer
from agentstate_reducer.models import ReducerConfig

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "gcdeveloper-new")
COLLECTION = "test_reducer_checkpoints"
THREAD_ID = "reducer-e2e-test-001"
MIN_MESSAGES = 4
MAX_MESSAGES = 6

model = ChatOpenAI(model="gpt-4o-mini", temperature=0)


def call_model(state: MessagesState):
    response = model.invoke(state["messages"])
    return {"messages": response}


def build_graph(checkpointer):
    builder = StateGraph(MessagesState)
    builder.add_node("call_model", call_model)
    builder.add_edge(START, "call_model")
    return builder.compile(checkpointer=checkpointer)


def test_reducer_caps_stored_messages():
    """Stored message count must not exceed min_messages + 1 (preserve_first adds 1)."""
    reducer = MessageReducer(config=ReducerConfig(min_messages=MIN_MESSAGES, max_messages=MAX_MESSAGES, preserve_first=True))
    with FirestoreSaver.from_conn_info(
        project_id=PROJECT_ID,
        checkpoints_collection=COLLECTION,
        reducer=reducer,
    ) as saver:
        graph = build_graph(saver)
        config = {"configurable": {"thread_id": THREAD_ID}}

        turns = [
            "Hi, my name is Kamal.",
            "I live in Pune.",
            "What is the capital of France?",
            "What is 2 + 2?",
            "Tell me a short joke.",
            "What colour is the sky?",
            "What did I say my name was?",
        ]
        for turn in turns:
            graph.invoke({"messages": [{"role": "user", "content": turn}]}, config)

        checkpoint_tuple = saver.get_tuple(config)
        assert checkpoint_tuple is not None
        stored_messages = checkpoint_tuple.checkpoint["channel_values"].get("messages", [])
        assert len(stored_messages) <= MIN_MESSAGES + 1, (
            f"Expected at most {MIN_MESSAGES + 1} messages stored, got {len(stored_messages)}"
        )


def test_reducer_preserves_recent_context():
    """Recent messages should be retained so the agent can answer questions about them."""
    reducer = MessageReducer(config=ReducerConfig(min_messages=MIN_MESSAGES, max_messages=MAX_MESSAGES, preserve_first=True))
    thread_id = THREAD_ID + "-context"
    with FirestoreSaver.from_conn_info(
        project_id=PROJECT_ID,
        checkpoints_collection=COLLECTION,
        reducer=reducer,
    ) as saver:
        graph = build_graph(saver)
        config = {"configurable": {"thread_id": thread_id}}

        graph.invoke({"messages": [{"role": "user", "content": "Hi, my name is Kamal."}]}, config)
        graph.invoke({"messages": [{"role": "user", "content": "What is 2 + 2?"}]}, config)
        graph.invoke({"messages": [{"role": "user", "content": "Tell me a short joke."}]}, config)

        result = graph.invoke(
            {"messages": [{"role": "user", "content": "What did I say my name was?"}]},
            config,
        )
        last_message = result["messages"][-1].content.lower()
        assert "kamal" in last_message, (
            f"Expected agent to recall 'kamal' from recent context, got: {last_message}"
        )
