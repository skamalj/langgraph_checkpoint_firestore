import asyncio
import getpass
import os

from asyncFirestoreSaver import AsyncFirestoreSaver
from langchain_openai import ChatOpenAI
from langgraph.graph import START, MessagesState, StateGraph


def _set_env(var: str):
    if not os.environ.get(var):
        os.environ[var] = getpass.getpass(f"{var}: ")


_set_env("OPENAI_API_KEY")
_set_env("GCP_PROJECT_ID")
model = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)


def call_model(state: MessagesState):
    response = model.invoke(state["messages"])
    return {"messages": response}


async def test_async():
    with AsyncFirestoreSaver.from_conn_info(
        project_id=os.environ["GCP_PROJECT_ID"],
        checkpoints_collection="langchain",
        writes_collection="langchain_writes",
    ) as memory:

        builder = StateGraph(MessagesState)
        builder.add_node("call_model", call_model)
        builder.add_edge(START, "call_model")
        graph = builder.compile(checkpointer=memory)

        config = {"configurable": {"thread_id": "10"}}
        # input_message = {"type": "user", "content": "hi! I'm Jeet"}
        # for chunk in graph.stream({"messages": [input_message]}, config, stream_mode="values"):
        #    chunk["messages"][-1].pretty_print()

        input_messages = [
            {
                "type": "system",
                "content": "You are a helpful assistant. User's name is John Doe. They live in London",
            },
            {"type": "user", "content": "what's my name?"},
        ]
        async for chunk in graph.astream(
            {"messages": input_messages}, config, stream_mode="values"
        ):
            chunk["messages"][-1].pretty_print()

        input_message = {"type": "user", "content": "Tell me history of my city?"}
        async for chunk in graph.astream(
            {"messages": [input_message]}, config, stream_mode="values"
        ):
            chunk["messages"][-1].pretty_print()


if __name__ == "__main__":
    asyncio.run(test_async())
