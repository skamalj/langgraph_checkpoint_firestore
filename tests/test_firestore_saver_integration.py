import uuid
import pytest
import os
from langgraph_checkpoint_firestore import FirestoreSaver
from langgraph.checkpoint.base import empty_checkpoint


def test_firestore_integration_workflow():
    # This test runs the actual FirestoreSaver logic against REAL Firestore.
    # It requires GCP_PROJECT_ID and authentication.

    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        pytest.skip("GCP_PROJECT_ID not provided, skipping integration test")

    # Ensure we are not using the mock (though this file doesn't import or patch it, just to be safe)
    # The environment variable REAL_FIRESTORE is no longer strictly needed for this file logic
    # as we don't switch between fake/real here, but user might still set it.

    unique_id = str(uuid.uuid4())
    collection_name = f"test_integration_{unique_id}"

    print("\nStarting Integration Test.")
    print(f"Project: {project_id}")
    print(f"Collection: {collection_name}")

    saver = FirestoreSaver(project_id=project_id, checkpoints_collection=collection_name)

    thread_id = "thread-1"
    checkpoint_ns = ""

    config = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": None,  # Initial
        }
    }

    # 2. Send something
    checkpoint = empty_checkpoint()
    checkpoint["channel_values"] = {"v": 1}

    metadata = {"source": "integration_test", "step": 1}

    # Put the checkpoint
    saved_config = saver.put(config, checkpoint, metadata, {})

    # 3. Check that it retrieves
    retrieved_tuple = saver.get_tuple(saved_config)

    assert retrieved_tuple is not None
    assert retrieved_tuple.checkpoint["channel_values"]["v"] == 1
    assert retrieved_tuple.metadata["source"] == "integration_test"
    assert retrieved_tuple.config["configurable"]["thread_id"] == thread_id

    # 4. Delete it
    saver.delete_thread(thread_id)

    # 5. Ensure it's deleted
    # Verify by trying to get the tuple again
    deleted_tuple = saver.get_tuple(saved_config)
    assert deleted_tuple is None

    # Verify list is empty
    checkpoints = list(saver.list(config))
    assert len(checkpoints) == 0
