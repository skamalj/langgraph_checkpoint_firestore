"""
E2E: FirestoreSaver + MessageReducer.on_prune -> LangGraph BaseStore.

Proves the chain  checkpointer.put -> reducer -> on_prune hook -> store.put
with a real compiled graph, a real Firestore collection for the checkpoints, and a
LangGraph ``BaseStore`` for long-term memory. The memory namespace comes from
``config["configurable"]["memory_namespace"]`` set by the app per invoke; the
checkpointer forwards it, the reducer forwards it, the hook writes to it.

Deliberately LLM-free: the single graph node echoes an AI reply so every
invoke adds exactly two messages (human + ai).

Requires GCP application-default credentials (GCP_PROJECT_ID, default gcdeveloper-new).

"""
import os
import uuid
from typing import Annotated, TypedDict

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.store.memory import InMemoryStore
from langgraph.store.base import BaseStore

from agentstate_reducer import Background, MessageReducer, ReducerConfig
from langgraph_checkpoint_firestore import FirestoreSaver

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "gcdeveloper-new")
COLLECTION = "test_on_prune_checkpoints"
MIN_MESSAGES = 4
MAX_MESSAGES = 6


class State(TypedDict):
    messages: Annotated[list, add_messages]


def _echo(state: State):
    last = state["messages"][-1]
    return {"messages": [AIMessage(content=f"echo: {last.content}")]}


def _remember_into(store: BaseStore):
    """The app's hook: write each pruned message into the store under the namespace it is handed."""

    def remember(pruned, namespace):
        assert namespace is not None, "checkpointer must forward a namespace"
        for m in pruned:
            store.put(
                tuple(namespace),
                key=str(uuid.uuid4()),
                value={"role": getattr(m, "type", None) or m.get("role"),
                       "content": getattr(m, "content", None) or m.get("content")},
            )

    return remember


def _build(store: BaseStore, hook):
    reducer = MessageReducer(config=ReducerConfig(
        min_messages=MIN_MESSAGES, max_messages=MAX_MESSAGES, preserve_first=False,
        on_prune=[hook],
    ))
    saver = FirestoreSaver(PROJECT_ID, COLLECTION, reducer=reducer)
    g = StateGraph(State)
    g.add_node("echo", _echo)
    g.add_edge(START, "echo")
    g.add_edge("echo", END)
    return g.compile(checkpointer=saver, store=store), saver


def _cleanup(saver, config):
    delete = getattr(saver, "delete", None)
    if callable(delete):
        delete(config)


def _run_turns(graph, config, n):
    for i in range(n):
        graph.invoke({"messages": [HumanMessage(content=f"turn {i}")]}, config=config)


def test_pruned_messages_land_in_store_under_app_namespace():
    store = InMemoryStore()
    graph, saver = _build(store, _remember_into(store))
    thread_id = f"t-{uuid.uuid4().hex[:8]}"          # throwaway thread id
    user_ns = ("memories", "kamal")                  # long-term scope is the USER
    config = {"configurable": {"thread_id": thread_id, "memory_namespace": user_ns}}

    _run_turns(graph, config, 5)                     # 10 messages -> several prunes

    # short-term: checkpoint holds only the retained window
    state = graph.get_state(config)
    assert len(state.values["messages"]) <= MAX_MESSAGES
    assert state.values["messages"][-1].content == "echo: turn 4"

    # long-term: everything pruned is in the store, under the user namespace
    items = store.search(user_ns, limit=100)
    contents = sorted(i.value["content"] for i in items)
    assert "turn 0" in contents and "echo: turn 0" in contents
    assert "echo: turn 4" not in contents            # still in the window, never pruned

    # nothing leaked into a thread-keyed namespace
    assert store.search(("memories", thread_id), limit=10) == []

    # window + store together cover every message ever sent, with no duplicates
    in_window = [m.content for m in state.values["messages"]]
    everything = sorted(contents + in_window)
    expected = sorted([f"turn {i}" for i in range(5)] + [f"echo: turn {i}" for i in range(5)])
    assert everything == expected

    _cleanup(saver, config)


def test_two_threads_same_user_share_one_memory_namespace():
    store = InMemoryStore()
    graph, saver = _build(store, _remember_into(store))
    user_ns = ("memories", "priya")
    cfgs = [{"configurable": {"thread_id": f"t-{uuid.uuid4().hex[:8]}", "memory_namespace": user_ns}}
            for _ in range(2)]
    for c in cfgs:
        _run_turns(graph, c, 4)
    contents = [i.value["content"] for i in store.search(user_ns, limit=100)]
    assert contents.count("turn 0") == 2             # one prune per thread, one namespace
    for c in cfgs:
        _cleanup(saver, c)


def test_missing_namespace_falls_back_to_thread():
    store = InMemoryStore()
    graph, saver = _build(store, _remember_into(store))
    thread_id = f"t-{uuid.uuid4().hex[:8]}"
    config = {"configurable": {"thread_id": thread_id}}   # app never set memory_namespace
    _run_turns(graph, config, 5)
    assert store.search(("memories", thread_id), limit=100)
    _cleanup(saver, config)


def test_background_hook_writes_to_store_off_path():
    store = InMemoryStore()
    bg = Background(_remember_into(store))
    graph, saver = _build(store, bg)
    user_ns = ("memories", "bg-user")
    config = {"configurable": {"thread_id": f"t-{uuid.uuid4().hex[:8]}", "memory_namespace": user_ns}}
    _run_turns(graph, config, 5)
    bg.close()                                        # drain before asserting
    assert bg.dropped == 0
    contents = [i.value["content"] for i in store.search(user_ns, limit=100)]
    assert "turn 0" in contents
    _cleanup(saver, config)


def test_custom_namespace_key():
    store = InMemoryStore()
    reducer = MessageReducer(config=ReducerConfig(
        min_messages=MIN_MESSAGES, max_messages=MAX_MESSAGES, preserve_first=False,
        on_prune=[_remember_into(store)], namespace_key="tenant",
    ))
    saver = FirestoreSaver(PROJECT_ID, COLLECTION, reducer=reducer)
    g = StateGraph(State); g.add_node("echo", _echo); g.add_edge(START, "echo"); g.add_edge("echo", END)
    graph = g.compile(checkpointer=saver, store=store)
    config = {"configurable": {"thread_id": f"t-{uuid.uuid4().hex[:8]}", "tenant": ("acme", "u9")}}
    _run_turns(graph, config, 5)
    assert store.search(("acme", "u9"), limit=100)
    _cleanup(saver, config)
