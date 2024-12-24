# LangGraph Checkpoint CosmosDB

This project provides an implementation of a checkpoint saver for LangGraph using Azure CosmosDB. 

## Features
- Save and retrieve langgraph checkpoints in Azure CosmosDB.

## Installation

To install the package, ensure you have Python 3.9 or higher, and run:

```pip install langgraph-checkpoint-cosmosdb```

## Usage

### Setting Up CosmosDBSaver

To use the `CosmosDBSaver`, you need to provide the CosmosDB endpoint and key, along with the database and container names. These can be set as environment variables:

```
export COSMOSDB_ENDPOINT='your_cosmosdb_endpoint'
export COSMOSDB_KEY='your_cosmosdb_key'
```

### Example

```
python
from langgraph_checkpoint_cosmosdb import CosmosDBSaver
```

# Initialize the saver
Database and Container is created if it does not exists
```
saver = CosmosDBSaver(database_name='your_database', container_name='your_container')
```

## Limitations
List function does not support filters. You can only pass config on thread id to get the list.

```
print(list(memory.list(config=config)))
```
## License

This project is licensed under the MIT License.
