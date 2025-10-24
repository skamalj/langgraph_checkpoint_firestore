from langchain_core.messages import SystemMessage
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt.chat_agent_executor import AgentStatePydantic

from langgraph_checkpoint_firestore import FirestoreSaver


class MainState(AgentStatePydantic):
    counter: int = 0


class SubgraphState(AgentStatePydantic):
    subgraph_counter: int = 0


def subgraph_node(state: SubgraphState):
    c = state.subgraph_counter + 1
    return {
        "subgraph_counter": c,
        "messages": [SystemMessage(content=f"The subgraph counter is at {c}")],
    }


def main_node(state: MainState):
    c = state.counter + 3
    return {
        "counter": c,
        "messages": [SystemMessage(content=f"The main counter is at {c}")],
    }


subgraph_builder = StateGraph(SubgraphState)
subgraph_builder.add_node("subgraph_node", subgraph_node)
subgraph_builder.add_edge(START, "subgraph_node")
subgraph_builder.add_edge("subgraph_node", END)
subgraph = subgraph_builder.compile(checkpointer=True)

builder = StateGraph(MainState)
builder.add_node("main_node", main_node)
builder.add_node("subgraph", subgraph)
builder.add_edge(START, "main_node")
builder.add_edge("main_node", "subgraph")
builder.add_edge("subgraph", END)

checkpointer = FirestoreSaver(
    project_id="gcdeveloper-new",
    checkpoints_collection="checkpoints-test",
    writes_collection="writes-test",
)

graph = builder.compile(checkpointer=checkpointer)

output = graph.invoke(
    {"messages": [SystemMessage(content="Begin the graph")]},
    config={"configurable": {"thread_id": "test5"}},
)

print(output)