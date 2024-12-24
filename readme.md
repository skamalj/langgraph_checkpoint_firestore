# LangGraph Checkpoint Firestore

This project provides an implementation of a checkpoint saver for LangGraph using Google Firestore. 

## Features
- Save and retrieve langgraph checkpoints in Google Firestore.

## Installation

To install the package, ensure you have Python 3.9 or higher, and run:

```pip install langgraph-checkpoint-firestore```

## Usage

### Setting Up CosmosDBSaver

To use the `FirestoreSaver`, you need to provide google default application authenmtication via environment 


### Example

```
python
from langgraph_checkpoint_firestore import FirestoreSaver
```

# Initialize the saver
Collections - write and checkpoint -  are created if it does not exists
```
saver = FirestoreSaver(project='project_id')
```

## Limitations
List function does not support filters. You can only pass config on thread id to get the list.

```
print(list(memory.list(config=config)))
```
## License

This project is licensed under the MIT License.
