# langgraph-checkpoint-firestore

Google Firestore checkpoint saver for [LangGraph](https://github.com/langchain-ai/langgraph). Persists agent state between runs so your graphs can resume from any prior checkpoint.

**What makes this checkpointer different:** it has message history pruning built in. Pass a `MessageReducer` and the checkpointer automatically caps your message list before writing to Firestore — no extra code in your graph, no state annotation changes required. This is the only LangGraph Firestore checkpointer with this capability.

## Features

- **Full checkpoint persistence** — save, retrieve, and list LangGraph checkpoints in Google Firestore
- **Built-in message pruning** — optional `MessageReducer` prunes message history at the persistence layer, keeping checkpoints lean without changing your graph code
- **Sync and async API** — `put`/`get_tuple`/`list` and their `aput`/`aget_tuple`/`alist` async counterparts
- **Subgraph support** — correctly checkpoints parent and subgraph state independently
- **Native Firestore hierarchy** — checkpoints stored in subcollections per thread, enabling efficient per-thread queries

## Installation

```bash
pip install langgraph-checkpoint-firestore
```

With optional message pruning support:

```bash
pip install "langgraph-checkpoint-firestore[reducer]"
```

**Requires Python 3.10+**

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
