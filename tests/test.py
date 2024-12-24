import getpass
import os, sys
from typing import Literal
from langchain_core.runnables import ConfigurableField
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from langgraph_checkpoint_firestore import FirestoreSaver
def _set_env(var: str):
    if not os.environ.get(var):
        os.environ[var] = getpass.getpass(f"{var}: ")


_set_env("OPENAI_API_KEY")
model = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)

from typing import Annotated
from typing_extensions import TypedDict

from langgraph.graph import StateGraph, MessagesState, START

memory = FirestoreSaver(project_id='gcdeveloper-new')

def call_model(state: MessagesState):
    response = model.invoke(state["messages"])
    return {"messages": response}


builder = StateGraph(MessagesState)
builder.add_node("call_model", call_model)
builder.add_edge(START, "call_model")
graph = builder.compile(checkpointer=memory)

config = {"configurable": {"thread_id": "10"}}
#input_message = {"type": "user", "content": "hi! I'm Jeet"}
#for chunk in graph.stream({"messages": [input_message]}, config, stream_mode="values"):
#    chunk["messages"][-1].pretty_print()

input_message = {"type": "user", "content": "what's my name?"}
for chunk in graph.stream({"messages": [input_message]}, config, stream_mode="values"):
    chunk["messages"][-1].pretty_print()

#input_message = {"type": "user", "content": "I live in Pune?"}
#for chunk in graph.stream({"messages": [input_message]}, config, stream_mode="values"):
#    chunk["messages"][-1].pretty_print()

input_message = {"type": "user", "content": "Tell me history of my place?"}
for chunk in graph.stream({"messages": [input_message]}, config, stream_mode="values"):
    chunk["messages"][-1].pretty_print()




    