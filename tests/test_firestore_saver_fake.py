import uuid
import pytest
from unittest.mock import patch
from langgraph_checkpoint_firestore import FirestoreSaver
from langgraph.checkpoint.base import (
    empty_checkpoint,
)


class FakeDocumentSnapshot:
    def __init__(self, data, exists=True, id=None, ref=None):
        self._data = data
        self.exists = exists
        self.id = id
        self.reference = ref

    def to_dict(self):
        return self._data


class FakeDocumentReference:
    def __init__(self, path, db):
        self.path = path
        self.id = path.split("/")[-1]
        self.db = db
        self._parent = None

    def collection(self, name):
        return FakeCollectionReference(f"{self.path}/{name}", self.db)

    def set(self, data):
        self.db._storage[self.path] = data

    def get(self):
        data = self.db._storage.get(self.path)
        return FakeDocumentSnapshot(data, exists=data is not None, id=self.id, ref=self)

    def delete(self):
        if self.path in self.db._storage:
            del self.db._storage[self.path]


class FakeCollectionReference:
    def __init__(self, path, db):
        self.path = path
        self.id = path.split("/")[-1]
        self.db = db

    def document(self, doc_id):
        return FakeDocumentReference(f"{self.path}/{doc_id}", self.db)

    def stream(self):
        results = []
        prefix = self.path + "/"
        for k, v in self.db._storage.items():
            if k.startswith(prefix):
                suffix = k[len(prefix) :]
                if "/" not in suffix:
                    results.append(
                        FakeDocumentSnapshot(
                            v,
                            exists=True,
                            id=suffix,
                            ref=FakeDocumentReference(k, self.db),
                        )
                    )
        return results

    def order_by(self, field, direction=None):
        return self

    def limit(self, count):
        return self


class FakeQuery:
    def __init__(self, collection_id, db):
        self.collection_id = collection_id
        self.db = db
        self.filters = []

    def where(self, filter):
        self.filters.append(filter)
        return self

    def stream(self):
        results = []
        for k, v in self.db._storage.items():
            parts = k.split("/")
            if len(parts) >= 2 and parts[-2] == self.collection_id:
                match = True
                for f in self.filters:
                    field = f.field_path
                    op = f.op_string
                    val = f.value

                    doc_val = v.get(field)
                    if doc_val is None:
                        match = False
                        break

                    if op == ">=":
                        if not (doc_val >= val):
                            match = False
                    elif op == "<":
                        if not (doc_val < val):
                            match = False
                    elif op == "==":
                        if not (doc_val == val):
                            match = False

                if match:
                    results.append(
                        FakeDocumentSnapshot(
                            v,
                            exists=True,
                            id=parts[-1],
                            ref=FakeDocumentReference(k, self.db),
                        )
                    )
        return results


class FakeBatch:
    def __init__(self, db):
        self.db = db
        self.ops = []

    def delete(self, ref):
        self.ops.append(("delete", ref))

    def set(self, ref, data):
        self.ops.append(("set", ref, data))

    def commit(self):
        for op in self.ops:
            if op[0] == "delete":
                op[1].delete()
            elif op[0] == "set":
                op[1].set(op[2])
        self.ops = []


class FakeFirestoreClient:
    def __init__(self, project):
        self.project = project
        self._storage = {}

    def collection(self, name):
        return FakeCollectionReference(name, self)

    def collection_group(self, collection_id):
        return FakeQuery(collection_id, self)

    def batch(self):
        return FakeBatch(self)


@pytest.fixture
def fake_firestore():
    with patch("langgraph_checkpoint_firestore.firestoreSaver.firestore.Client") as mock_client_cls:
        fake_db = FakeFirestoreClient("test-project")
        mock_client_cls.return_value = fake_db
        yield fake_db


def test_firestore_fake_workflow(fake_firestore):
    unique_id = str(uuid.uuid4())
    collection_name = f"test_fake_{unique_id}"
    project_id = "test-project"

    saver = FirestoreSaver(project_id=project_id, checkpoints_collection=collection_name)

    thread_id = "thread-1"
    checkpoint_ns = ""

    config = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": None,
        }
    }

    checkpoint = empty_checkpoint()
    checkpoint["channel_values"] = {"v": 1}

    metadata = {"source": "fake_test", "step": 1}

    # Put
    saved_config = saver.put(config, checkpoint, metadata, {})

    # White-box check
    ckpt_id = saved_config["configurable"]["checkpoint_id"]
    expected_path = f"{collection_name}/{thread_id}_/checkpoints/{ckpt_id}"
    assert expected_path in fake_firestore._storage

    # Get
    retrieved_tuple = saver.get_tuple(saved_config)
    assert retrieved_tuple is not None
    assert retrieved_tuple.checkpoint["channel_values"]["v"] == 1

    # Delete
    saver.delete_thread(thread_id)

    # Verify deleted
    deleted_tuple = saver.get_tuple(saved_config)
    assert deleted_tuple is None
    assert expected_path not in fake_firestore._storage

    checkpoints = list(saver.list(config))
    assert len(checkpoints) == 0
