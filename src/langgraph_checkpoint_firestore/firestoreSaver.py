# create firestore Saver (langgraph checkpointer) basis cosmosdb implementation in included code include=src/langgraph_checkpoint_firestore/cosmosdbSaver.py 
#  Remember there is no partition key in firestore only unique ID.
# create separate collection for checkpoints and writes. Next level collection will be thread_id and then checkpoint_id
# we have to preserve the function signatures and return values (as in cosmosdbsaver) as it is.
# Do provide full and complete code, i.e all function along with Saver in the included file.
# @!

from contextlib import contextmanager
from typing import Any, Iterator, List, Optional, Tuple

from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import WRITES_IDX_MAP, BaseCheckpointSaver, ChannelVersions, Checkpoint, CheckpointMetadata, CheckpointTuple, PendingWrite, get_checkpoint_id
from langgraph.checkpoint.serde.base import SerializerProtocol
from google.cloud import firestore


FIRESTORE_KEY_SEPARATOR = "/"

def _make_firestore_checkpoint_key(thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> str:
    return FIRESTORE_KEY_SEPARATOR.join([
        "checkpoint", thread_id, checkpoint_ns, checkpoint_id
    ])


def _make_firestore_checkpoint_writes_key(thread_id: str, checkpoint_ns: str, checkpoint_id: str, task_id: str, idx: Optional[int]) -> str:
    if idx is None:
        return FIRESTORE_KEY_SEPARATOR.join([
            "writes", thread_id, checkpoint_ns, checkpoint_id, task_id
        ])

    return FIRESTORE_KEY_SEPARATOR.join([
        "writes", thread_id, checkpoint_ns, checkpoint_id, task_id, str(idx)
    ])


def _parse_firestore_checkpoint_key(cosmosdb_key: str) -> dict:
    namespace, thread_id, checkpoint_ns, checkpoint_id = cosmosdb_key.split(
        FIRESTORE_KEY_SEPARATOR
    )
    if namespace != "checkpoint":
        raise ValueError("Expected checkpoint key to start with 'checkpoint'")

    return {
        "thread_id": thread_id,
        "checkpoint_ns": checkpoint_ns,
        "checkpoint_id": checkpoint_id,
    }


def _parse_firestore_checkpoint_writes_key(cosmosdb_key: str) -> dict:
    namespace, thread_id, checkpoint_ns, checkpoint_id, task_id, idx = cosmosdb_key.split(
        FIRESTORE_KEY_SEPARATOR
    )
    if namespace != "writes":
        raise ValueError("Expected checkpoint key to start with 'writes'")

    return {
        "thread_id": thread_id,
        "checkpoint_ns": checkpoint_ns,
        "checkpoint_id": checkpoint_id,
        "task_id": task_id,
        "idx": idx,
    }


def _filter_keys(keys: List[str], before: Optional[RunnableConfig], limit: Optional[int]) -> list:
    if before:
        keys = [
            k
            for k in keys
            if _parse_firestore_checkpoint_key(k)["checkpoint_id"]
            < before["configurable"]["checkpoint_id"]
        ]

    keys = sorted(
        keys,
        key=lambda k: _parse_firestore_checkpoint_key(k)["checkpoint_id"],
        reverse=True,
    )
    if limit:
        keys = keys[:limit]
    return keys


def _load_writes(serde: SerializerProtocol, task_id_to_data: dict[tuple[str, str], dict]) -> list[PendingWrite]:
    writes = [
        (
            task_id,
            data["channel"],
            serde.loads_typed((data["type"], data["value"])),
        )
        for (task_id, _), data in task_id_to_data.items()
    ]
    return writes


def _parse_firestore_checkpoint_data(serde: SerializerProtocol, key: str, data: dict, pending_writes: Optional[List[PendingWrite]] = None) -> Optional[CheckpointTuple]:
    if not data:
        return None

    parsed_key = _parse_firestore_checkpoint_key(key)
    thread_id = parsed_key["thread_id"]
    checkpoint_ns = parsed_key["checkpoint_ns"]
    checkpoint_id = parsed_key["checkpoint_id"]
    config = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
        }
    }

    checkpoint = serde.loads_typed((data["type"], data["checkpoint"]))
    metadata = serde.loads(data["metadata"])
    parent_checkpoint_id = data.get("parent_checkpoint_id", "")
    parent_config = (
        {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": parent_checkpoint_id,
            }
        }
        if parent_checkpoint_id
        else None
    )
    return CheckpointTuple(
        config=config,
        checkpoint=checkpoint,
        metadata=metadata,
        parent_config=parent_config,
        pending_writes=pending_writes,
    )

class FirestoreSaver(BaseCheckpointSaver):
    def __init__(self):
        super().__init__()
        self.firestore_serde = SerializerProtocol(self.serde)
        self.client = firestore.Client()
        self.checkpoints_collection = self.client.collection('checkpoints')
        self.writes_collection = self.client.collection('writes')

    @classmethod
    @contextmanager
    def from_conn_info(cls) -> Iterator['FirestoreSaver']:
        saver = None
        try:
            saver = FirestoreSaver()
            yield saver
        finally:
            pass

    def put(self, config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata, new_versions: ChannelVersions) -> RunnableConfig:
        thread_id = config['configurable']['thread_id']
        checkpoint_ns = config['configurable']['checkpoint_ns']
        if not checkpoint_ns: 
            checkpoint_ns = "default"
        checkpoint_id = checkpoint['id']
        parent_checkpoint_id = config['configurable'].get('checkpoint_id')
        key = _make_firestore_checkpoint_key(thread_id, checkpoint_ns, checkpoint_id)

        type_, serialized_checkpoint = self.firestore_serde.dumps_typed(checkpoint)
        serialized_metadata = self.firestore_serde.dumps(metadata)
        data = {
            'checkpoint': serialized_checkpoint,
            "checkpoint_key": key,
            'type': type_,
            'metadata': serialized_metadata,
            'parent_checkpoint_id': parent_checkpoint_id if parent_checkpoint_id else ''
        }
        self.checkpoints_collection.document(thread_id).collection(checkpoint_id).document('data').set(data)
        return {
            'configurable': {
                'thread_id': thread_id,
                'checkpoint_ns': checkpoint_ns,
                'checkpoint_id': checkpoint_id
            }
        }

    def put_writes(self, config: RunnableConfig, writes: List[Tuple[str, Any]], task_id: str) -> None:
        thread_id = config['configurable']['thread_id']
        checkpoint_ns = config['configurable']['checkpoint_ns']
        if not checkpoint_ns: 
            checkpoint_ns = "default"
        checkpoint_id = config['configurable']['checkpoint_id']

        for idx, (channel, value) in enumerate(writes):
            key = _make_firestore_checkpoint_writes_key(
                thread_id,
                checkpoint_ns,
                checkpoint_id,
                task_id,
                WRITES_IDX_MAP.get(channel, idx),
            )
            type_, serialized_value = self.serde.dumps_typed(value)
            data = {"checkpoint_key": key, 'channel': channel, 'type': type_, 'value': serialized_value}
            self.writes_collection.document(thread_id).collection(checkpoint_id).document(task_id).set(data)

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        thread_id = config['configurable']['thread_id']
        checkpoint_id = get_checkpoint_id(config)
        checkpoint_ns = config['configurable'].get('checkpoint_ns', 'default')

        checkpoint_key = self._get_checkpoint_key(
            self.container, thread_id, checkpoint_ns, checkpoint_id
        )
        if not checkpoint_key:
            return None

        doc_ref = self.checkpoints_collection.document(thread_id).collection(checkpoint_id).document('data')
        doc = doc_ref.get()
        if not doc.exists:
            return None

        checkpoint_data = doc.to_dict()
        pending_writes = self._load_pending_writes(
            thread_id, checkpoint_ns, checkpoint_id
        )
        return _parse_firestore_checkpoint_data(
            self.serde, checkpoint_key, checkpoint_data, pending_writes=pending_writes
        )

    def list(self, config: Optional[RunnableConfig], *, filter: Optional[dict[str, Any]] = None, before: Optional[RunnableConfig] = None, limit: Optional[int] = None) -> Iterator[CheckpointTuple]:
        thread_id = config['configurable']['thread_id']
        checkpoint_ns = config['configurable'].get('checkpoint_ns', 'default')

        checkpoints = self.checkpoints_collection.document(thread_id).collections()
        for checkpoint in checkpoints:
            checkpoint_id = checkpoint.id
            doc_ref = checkpoint.document('data')
            doc = doc_ref.get()
            if doc.exists:
                checkpoint_data = doc.to_dict()
                pending_writes = self._load_pending_writes(thread_id, checkpoint_ns, checkpoint_id)
                yield self._parse_firestore_checkpoint_data(self.serde, checkpoint_data["checkpoint_key"],checkpoint_data, pending_writes)

    def _load_pending_writes(self, thread_id: str, checkpoint_ns: Optional[str] , checkpoint_id: str) -> List[PendingWrite]:
        writes = []
        writes_ref = self.writes_collection.document(thread_id).collection(checkpoint_id)  # Use collection instead of collections()
        try:
            for write_doc in writes_ref.stream():  # Stream through documents in the collection
                write_data = write_doc.to_dict()
                task_id = write_data.get('task_id', None)  # Use .get() for optional value

                if task_id:
                    # Assuming write_data has 'channel', 'type', and 'value' fields
                    try:
                        deserialized_value = self.firestore_serde.loads_typed((write_data['type'], write_data['value']))
                        writes.append(PendingWrite(task_id, write_data['channel'], deserialized_value))
                    except Exception as e:
                        print(f"Error deserializing write data for task {task_id}: {e}")

        except Exception as e:
            print(f"Error querying Firestore: {e}")

        return writes
   
    def _get_checkpoint_key(self, thread_id: str, checkpoint_ns: str, checkpoint_id: Optional[str]) -> Optional[str]:
        if not checkpoint_ns: 
            checkpoint_ns = "default"
        if checkpoint_id:
            return _make_firestore_checkpoint_key(thread_id, checkpoint_ns, checkpoint_id)

        collection_ref = self.db.collection("checkpoints").document(thread_id).collection(checkpoint_ns)

        try:
            docs = list(collection_ref.stream())  # Get all documents in the subcollection
        except Exception as e:
            print(f"Error querying Firestore: {e}")
            return None

        if not docs:
            return None

        latest_doc = max(docs, key=lambda doc: _parse_firestore_checkpoint_key(doc.to_dict()["checkpoint_key"])["checkpoint_id"])
        return latest_doc.to_dict()["checkpoint_key"]