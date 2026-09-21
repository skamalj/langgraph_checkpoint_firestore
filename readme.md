# langgraph-checkpoint-firestore

A LangGraph checkpointer for **Google Firestore** with two things a plain checkpointer does not have: **bounded message history** and a **long-term memory hook**, both inside the save path, with no change to your graph.

<p align="center"><img src="https://raw.githubusercontent.com/skamalj/langgraph_checkpoint_firestore/main/docs/agentstate-flow.svg" width="100%" alt="Messages accumulate in the checkpoint until the window's upper bound, the reducer prunes back to the lower bound, and the pruned turns flow through the on_prune hook into a long-term store"></p>

- **Bounded history.** Every thread's message list is pruned before each checkpoint is written, by message count or token budget, whole messages only, tool-call pairs kept intact. The sawtooth above is the checkpoint size over time. Without a reducer it is a straight line up.
- **Long-term memory.** The turns that leave the window are handed to the reducer's `on_prune` hook, once each, together with the `memory_namespace` your app put in the run config. Wire that hook to any store or extraction engine; [`langgraph-memory`](https://pypi.org/project/langgraph-memory/) is the ready-made one.
- **One line of config.**

```python
from agentstate_reducer import MessageReducer, ReducerConfig, Background
from langgraph_checkpoint_firestore import FirestoreSaver

reducer = MessageReducer(config=ReducerConfig(max_messages=20))            # add on_prune=[Background(engine.on_prune)] for memory
saver = FirestoreSaver("my-gcp-project", "checkpoints", reducer=reducer)
graph = builder.compile(checkpointer=saver)

graph.invoke(input, config={"configurable": {"thread_id": thread_id, "memory_namespace": ("memories", user_id)}})
```

Details: [Built-in Message Pruning](#built-in-message-pruning) below, and the full story with every framework and store at [https://skamalj.github.io/agentstate-reducer/](https://skamalj.github.io/agentstate-reducer/langgraph/firestore/).

## Installation

```bash
pip install "langgraph-checkpoint-firestore[reducer]"
```

```bash
pip install langgraph-checkpoint-firestore     # checkpointer only; history is unbounded
```

Without `reducer=`, the saver logs one INFO line per process saying so, with the link above. Set `AGENTSTATE_QUIET=1` to silence it.

**Requires Python 3.10+.** Application Default Credentials, per-thread subcollections, sync and async, subgraphs. Passes **all eight capabilities** of `langgraph-checkpoint-conformance` (base plus `copy_thread`, `delete_for_runs`, `prune`), which is what LangSmith Deployment probes at startup; see [LangSmith Deployment](#langsmith-deployment).

## Firestore Setup

The saver uses Google Cloud Application Default Credentials. Set up auth with one of:

```bash
# Local development — authenticate with your Google account
gcloud auth application-default login

# Service account (CI / production)
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

Your Firestore instance must be in **Native mode** (not Datastore mode). Collections are created automatically on first write — no manual setup required.

## Quick Start

```python
from langgraph.graph import StateGraph, MessagesState, START
from langchain_openai import ChatOpenAI
from langgraph_checkpoint_firestore import FirestoreSaver

model = ChatOpenAI(model="gpt-4o-mini")

def call_model(state: MessagesState):
    return {"messages": model.invoke(state["messages"])}

builder = StateGraph(MessagesState)
builder.add_node("call_model", call_model)
builder.add_edge(START, "call_model")

with FirestoreSaver.from_conn_info(project_id="my-gcp-project", checkpoints_collection="checkpoints") as checkpointer:
    graph = builder.compile(checkpointer=checkpointer)

    config = {"configurable": {"thread_id": "user-123"}}

    # First run — state is saved to Firestore
    graph.invoke({"messages": [{"role": "user", "content": "Hi, I'm Kamal"}]}, config)

    # Second run — picks up where it left off
    graph.invoke({"messages": [{"role": "user", "content": "What's my name?"}]}, config)
```

## API Reference

### `FirestoreSaver(project_id, checkpoints_collection, reducer=None, messages_key="messages")`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `project_id` | `str` | required | Google Cloud project ID |
| `checkpoints_collection` | `str` | `"checkpoints"` | Root Firestore collection name |
| `reducer` | `MessageReducer` | `None` | Optional pruner — see [Message Pruning](#built-in-message-pruning) |
| `messages_key` | `str` | `"messages"` | State channel name that holds the message list |

You can also use the context manager factory:

```python
with FirestoreSaver.from_conn_info(
    project_id="my-gcp-project",
    checkpoints_collection="checkpoints",
    reducer=reducer,
    messages_key="messages"
) as saver:
    graph = builder.compile(checkpointer=saver)
```

### Sync methods

| Method | Description |
|---|---|
| `put(config, checkpoint, metadata, new_versions)` | Save a checkpoint |
| `put_writes(config, writes, task_id)` | Save pending writes for a checkpoint |
| `get_tuple(config)` | Retrieve the latest (or a specific) checkpoint |
| `list(config, *, before, limit)` | Iterate checkpoints for a thread |

### Async methods

All sync methods have async counterparts: `aput`, `aput_writes`, `aget_tuple`, `alist`.

## LangSmith Deployment

LangSmith Deployment (Agent Server) accepts a custom checkpointer through `langgraph.json` and checks its capabilities at startup. This saver implements the full set, so thread forking (`copy_thread`), the rollback multitask strategy (`delete_for_runs`) and history pruning (`prune`) are all available.

```json
{
  "dependencies": ["."],
  "graphs": {"agent": "./src/agent/graph.py:graph"},
  "checkpointer": {"path": "./src/agent/checkpointer.py:generate_checkpointer"}
}
```

```python
# src/agent/checkpointer.py
from contextlib import asynccontextmanager
from agentstate_reducer import MessageReducer, ReducerConfig
from langgraph_checkpoint_firestore import FirestoreSaver

@asynccontextmanager
async def generate_checkpointer():
    reducer = MessageReducer(config=ReducerConfig(max_messages=20))
    yield FirestoreSaver("my-gcp-project", "checkpoints", reducer=reducer)
```

Housekeeping methods, also usable outside the platform:

```python
saver.prune([thread_id], strategy="keep_latest")   # keep only the newest checkpoint per namespace
saver.delete_for_runs([run_id])                    # remove everything a run wrote
saver.copy_thread(thread_id, new_thread_id)        # fork a conversation
```

`prune` is not `DeltaChannel`-aware; do not use `keep_latest` on threads whose graph uses `DeltaChannel`. `delete_for_runs` finds checkpoints by a `run_id` attribute written since this version; older checkpoints are not matched.

## Built-in Message Pruning

Long-running agents accumulate message history with every turn. Left unchecked this inflates checkpoint size, increases Firestore storage costs, and eventually blows past LLM context limits.

This checkpointer solves that at the persistence layer: pass a `MessageReducer` and it automatically prunes the message list inside `put()` before the checkpoint is serialised and written to Firestore. **Your graph code, state definition, and node logic stay untouched.**

This is an alternative to — or complement of — the LangGraph `Annotated[list, reducer_fn]` pattern. Use the checkpoint-layer approach when:

- You don't own the graph or state definition (e.g. using a pre-built LangGraph agent)
- You want pruning to happen unconditionally at every save, regardless of which node triggered it
- You want to keep all in-memory state intact and only prune what gets persisted

### Install with reducer support

```bash
pip install "langgraph-checkpoint-firestore[reducer]"
```

### Usage

```python
from agentstate_reducer import MessageReducer
from langgraph_checkpoint_firestore import FirestoreSaver

reducer = MessageReducer(min_messages=10, max_messages=20)

with FirestoreSaver.from_conn_info(
    project_id="my-gcp-project",
    checkpoints_collection="checkpoints",
    reducer=reducer,        # prune before each checkpoint save
    messages_key="messages" # state channel holding the message list (default)
) as checkpointer:
    graph = builder.compile(checkpointer=checkpointer)
```

When `len(messages) > max_messages`, the oldest `human`/`ai` messages are removed until `min_messages` remain. The following are **never** pruned:

- Index 0 (typically the system prompt) — controlled by `preserve_first=True`
- `system` and `function` messages
- `tool` messages — unless their parent `ai` message is pruned (cascade behaviour, configurable)

See [agentstate-reducer on PyPI](https://pypi.org/project/agentstate-reducer/) for full configuration: `preserve_first`, `cascade_tool_messages`, `summarize_fn`, and role alias support (`user`/`assistant`/`agent`).

### Usage — long-term memory via `on_prune` (agentstate-reducer >= 0.4.0)

Messages pruned from the checkpoint are exactly the ones leaving the model's view. The saver forwards a **memory namespace** to the reducer, and any `on_prune` hook receives `(pruned_messages, namespace)` — so pruned turns can flow straight into a LangGraph `BaseStore` (or LangMem, or any memory engine) with no extra node and no package coupling:

```python
from uuid import uuid4
from agentstate_reducer import MessageReducer, ReducerConfig, Background
from langgraph_checkpoint_firestore import FirestoreSaver

store = ...  # any langgraph BaseStore

def remember(pruned, namespace):
    for m in pruned:
        store.put(tuple(namespace), key=str(uuid4()), value={"role": m.type, "content": m.content})

reducer = MessageReducer(config=ReducerConfig(max_messages=20, on_prune=[remember]))
# on_prune=[Background(remember)] runs a slow hook (e.g. LLM extraction) off the request path
saver = FirestoreSaver("my-project", "checkpoints", reducer=reducer)
graph = builder.compile(checkpointer=saver, store=store)

graph.invoke(input, config={"configurable": {
    "thread_id": uuid4().hex,                    # short-term scope (this checkpoint)
    "memory_namespace": ("memories", user_id),   # long-term scope (the store)
}})
```

The saver reads `memory_namespace` (or whatever `ReducerConfig.namespace_key` names) from `config["configurable"]` on every `put()` and passes it through untouched. If the app never sets it, the namespace falls back to `("memories", thread_id)`. Each pruned message reaches the hooks once, even though LangGraph writes several checkpoints per turn.


## Data Model

Checkpoints are stored in a hierarchical Firestore structure:

```
{checkpoints_collection}/
  {thread_id}_{checkpoint_ns}/          ← partition document
    checkpoints/
      {checkpoint_id}                   ← checkpoint document
        writes/
          {task_id}_{idx}               ← pending write documents
```

This structure enables efficient per-thread checkpoint queries and keeps checkpoint data co-located with its pending writes.

## License

MIT
