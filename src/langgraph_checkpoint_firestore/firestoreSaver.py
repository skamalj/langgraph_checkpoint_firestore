# create firestore Saver (langgraph checkpointer) basis firestoredb implementation in included code include=src/langgraph_checkpoint_firestore/firestoredbSaver.py 
#  Remember there is no partition key in firestore only unique ID.
# create separate collection for checkpoints and writes. Next level collection will be thread_id and then checkpoint_id
# we have to preserve the function signatures and return values (as in firestoredbsaver) as it is.
# Do provide full and complete code, i.e all function along with Saver in the included file.
# @!

import copy
from contextlib import contextmanager
from typing import Dict, Any, Iterator, List, Optional, Tuple, AsyncIterator

from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import WRITES_IDX_MAP, BaseCheckpointSaver, ChannelVersions, Checkpoint, CheckpointMetadata, CheckpointTuple, PendingWrite, get_checkpoint_id

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from langgraph_checkpoint_firestore.firestoreSerializer import FirestoreSerializer
from langgraph_checkpoint_firestore._nudge import nudge_unbounded_history
import logging
logger = logging.getLogger("langgraph_checkpoint_firestore")
import asyncio

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


def _parse_firestore_checkpoint_key(firestoredb_key: str) -> dict:
    namespace, thread_id, checkpoint_ns, checkpoint_id = firestoredb_key.split(
        FIRESTORE_KEY_SEPARATOR
    )
    if namespace != "checkpoint":
        raise ValueError("Expected checkpoint key to start with 'checkpoint'")

    return {
        "thread_id": thread_id,
        "checkpoint_ns": checkpoint_ns,
        "checkpoint_id": checkpoint_id,
    }


def _parse_firestore_checkpoint_writes_key(firestoredb_key: str) -> dict:
    namespace, thread_id, checkpoint_ns, checkpoint_id, task_id, idx = firestoredb_key.split(
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


def _load_writes(serde: FirestoreSerializer, task_id_to_data: dict[tuple[str, str], dict]) -> list[PendingWrite]:
    writes = [
        (
            task_id,
            data["channel"],
            serde.loads_typed((data["type"], data["value"])),
        )
        for (task_id, _), data in task_id_to_data.items()
    ]
    return writes


def _parse_firestore_checkpoint_data(serde: FirestoreSerializer, key: str, data: dict, pending_writes: Optional[List[PendingWrite]] = None) -> Optional[CheckpointTuple]:
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
    
    metadata = serde.loads_typed(data["metadata"])
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
    def __init__(self, project_id, checkpoints_collection='checkpoints', reducer=None, messages_key="messages"):
        super().__init__()
        self.client = firestore.Client(project=project_id)
        self.firestore_serde = FirestoreSerializer(self.serde)
        self.checkpoints_collection = self.client.collection(checkpoints_collection)
        self.reducer = reducer
        self.messages_key = messages_key
        if reducer is None:
            nudge_unbounded_history(logger)

    @classmethod
    @contextmanager
    def from_conn_info(cls, *, project_id: str, checkpoints_collection: str, reducer=None, messages_key="messages", **kwargs) -> Iterator['FirestoreSaver']:
        saver = None
        try:
            saver = FirestoreSaver(project_id, checkpoints_collection, reducer=reducer, messages_key=messages_key)
            yield saver
        finally:
            pass

    def _memory_namespace(self, config: RunnableConfig):
        """Resolve the long-term-memory namespace forwarded to reducer ``on_prune`` hooks.

        Looks up ``reducer.config.namespace_key`` (default ``"memory_namespace"``)
        in ``config["configurable"]``; the app sets it per invoke, e.g.
        ``{"thread_id": ..., "memory_namespace": ("memories", user_id)}``.
        Falls back to ``("memories", thread_id)`` so apps that never set it
        still get per-thread memory. The checkpointer never builds the namespace
        itself beyond that fallback.
        """
        conf = config.get("configurable", {}) if config else {}
        key = getattr(getattr(self.reducer, "config", None), "namespace_key", "memory_namespace")
        ns = conf.get(key)
        if ns is not None:
            return ns
        thread_id = conf.get("thread_id")
        return ("memories", thread_id) if thread_id is not None else None

    def _apply_reducer(self, checkpoint: Checkpoint, config: Optional[RunnableConfig] = None) -> Checkpoint:
        """Return a checkpoint with the messages channel reduced (non-mutating).

        The memory namespace resolved from ``config`` is forwarded to the reducer
        so ``on_prune`` hooks can write pruned messages to long-term memory
        (agentstate-reducer >= 0.4.0; older reducers ignore it).
        """
        if self.reducer is None:
            return checkpoint
        channel_values = checkpoint.get("channel_values", {})
        messages = channel_values.get(self.messages_key)
        if not messages:
            return checkpoint
        try:
            result = self.reducer.reduce(
                existing=messages, new=[], namespace=self._memory_namespace(config)
            )
        except TypeError:  # agentstate-reducer < 0.4.0: no namespace kwarg
            result = self.reducer.reduce(existing=messages, new=[])
        new_channel_values = dict(channel_values)
        new_channel_values[self.messages_key] = result.surviving
        new_checkpoint = copy.copy(checkpoint)
        new_checkpoint["channel_values"] = new_channel_values
        return new_checkpoint

    # Helper to get subcollection for a given partition
    def _get_partition_collection(self, thread_id: str, checkpoint_ns: str):
        """Return the Firestore collection reference representing one partition (thread+ns)."""
        partition_doc = self.checkpoints_collection.document(f"{thread_id}_{checkpoint_ns}")
        return partition_doc.collection("checkpoints")
    
    def put(self, config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata, new_versions: ChannelVersions) -> RunnableConfig:
        checkpoint = self._apply_reducer(checkpoint, config)
        thread_id = config['configurable']['thread_id']
        checkpoint_ns = config['configurable']['checkpoint_ns']
        checkpoint_id = checkpoint['id']
        parent_checkpoint_id = config['configurable'].get('checkpoint_id')
        key = _make_firestore_checkpoint_key(thread_id, checkpoint_ns, checkpoint_id)

        type_, serialized_checkpoint = self.firestore_serde.dumps_typed(checkpoint)
        serialized_metadata = self.firestore_serde.dumps_typed(metadata)
        data = {
            'checkpoint': serialized_checkpoint,
            'checkpoint_id': checkpoint_id,
            "checkpoint_key": key,
            'type': type_,            
            'metadata': serialized_metadata,
            'parent_checkpoint_id': parent_checkpoint_id if parent_checkpoint_id else ''
        }
        # Top-level copy of metadata.run_id so delete_for_runs can query by it
        # (single-field automatic index; no composite index needed).
        run_id = metadata.get("run_id") if metadata else None
        if run_id is not None:
            data["run_id"] = run_id
        partition_collection = self._get_partition_collection(thread_id, checkpoint_ns)
        partition_collection.document(checkpoint_id).set(data)
        return {
            'configurable': {
                'thread_id': thread_id,
                'checkpoint_ns': checkpoint_ns,
                'checkpoint_id': checkpoint_id
            }
        }

    def put_writes(self, config: RunnableConfig, writes: List[Tuple[str, Any]], task_id: str, task_path: str = "") -> None:
        thread_id = config['configurable']['thread_id']
        checkpoint_ns = config['configurable']['checkpoint_ns']
        checkpoint_id = config['configurable']['checkpoint_id']

        # Writes belong under the checkpoint itself
        partition_collection = self._get_partition_collection(thread_id, checkpoint_ns)
        # self.write_collection is not used anymore. 
        writes_collection = partition_collection.document(checkpoint_id).collection("writes")
  
        for idx, (channel, value) in enumerate(writes):
            key = _make_firestore_checkpoint_writes_key(
                thread_id,
                checkpoint_ns,
                checkpoint_id,
                task_id,
                WRITES_IDX_MAP.get(channel, idx),
            )
            type_, serialized_value = self.firestore_serde.dumps_typed(value)
            data = {"checkpoint_key": key, 'channel': channel, 'type': type_, 
                    'value': serialized_value, 
                    "task_id": task_id,
                    "idx": WRITES_IDX_MAP.get(channel, idx)}
            data["task_path"] = task_path
            writes_collection.document(f"{task_id}_{WRITES_IDX_MAP.get(channel, idx)}").set(data)

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        thread_id = config['configurable']['thread_id']
        checkpoint_id = get_checkpoint_id(config)
        
        checkpoint_ns = config['configurable'].get('checkpoint_ns', '')

        checkpoint_key = self._get_checkpoint_key(
            thread_id, checkpoint_ns, checkpoint_id
        )

        if not checkpoint_key:
            return None
        checkpoint_id = _parse_firestore_checkpoint_key(checkpoint_key)["checkpoint_id"]
        partition_collection = self._get_partition_collection(thread_id, checkpoint_ns)

        doc_ref = partition_collection.document(checkpoint_id)
        doc = doc_ref.get()
        if not doc.exists:
            return None

        checkpoint_data = doc.to_dict()
        
        pending_writes = self._load_pending_writes(
            thread_id, checkpoint_ns, checkpoint_id
        )
        return _parse_firestore_checkpoint_data(
            self.firestore_serde, checkpoint_key, checkpoint_data, pending_writes=pending_writes
        )

    def list(self, config: Optional[RunnableConfig], *, filter: Optional[dict[str, Any]] = None, before: Optional[RunnableConfig] = None, limit: Optional[int] = None) -> Iterator[CheckpointTuple]:
        thread_id = config['configurable']['thread_id']
        checkpoint_ns = config['configurable'].get('checkpoint_ns', '')

        partition_collection = self._get_partition_collection(thread_id, checkpoint_ns)

        # Order by checkpoint_id descending (latest first); `before`, metadata filter and
        # limit are applied here (limit after filtering, so it caps *matching* checkpoints).
        query = partition_collection.order_by("checkpoint_id", direction=firestore.Query.DESCENDING)
        before_id = get_checkpoint_id(before) if before else None
        if before_id is not None:
            query = query.where(filter=FieldFilter("checkpoint_id", "<", before_id))
        yielded = 0
        for checkpoint in query.stream():
            if not checkpoint.exists:
                continue
            checkpoint_data = checkpoint.to_dict()
            checkpoint_id = checkpoint_data["checkpoint_id"]
            pending_writes = self._load_pending_writes(thread_id, checkpoint_ns, checkpoint_id)
            tup = _parse_firestore_checkpoint_data(self.firestore_serde, checkpoint_data["checkpoint_key"], checkpoint_data, pending_writes)
            if tup is None:
                continue
            if filter and not all(tup.metadata.get(k) == v for k, v in filter.items()):
                continue
            yield tup
            yielded += 1
            if limit is not None and yielded >= limit:
                return

    def delete_thread(self, thread_id: str) -> None:
        """Delete every checkpoint and pending write for a thread, across namespaces."""
        prefix = f"{thread_id}_"
        for partition_doc in self.checkpoints_collection.list_documents():
            if not partition_doc.id.startswith(prefix):
                continue
            for cp_doc in partition_doc.collection("checkpoints").list_documents():
                for w in cp_doc.collection("writes").list_documents():
                    w.delete()
                cp_doc.delete()
            partition_doc.delete()

    async def adelete_thread(self, thread_id: str) -> None:
        await asyncio.get_running_loop().run_in_executor(None, self.delete_thread, thread_id)

    # ------------------------------------------------------------------
    # Optional capabilities: copy_thread / delete_for_runs / prune
    # ------------------------------------------------------------------

    _BATCH_LIMIT = 400  # Firestore caps a WriteBatch at 500 operations

    class _Batcher:
        """Accumulate batch operations and commit every ``limit`` ops."""

        def __init__(self, client, limit):
            self.client = client
            self.limit = limit
            self.batch = client.batch()
            self.count = 0

        def _tick(self):
            self.count += 1
            if self.count >= self.limit:
                self.flush()

        def set(self, ref, data):
            self.batch.set(ref, data)
            self._tick()

        def delete(self, ref):
            self.batch.delete(ref)
            self._tick()

        def flush(self):
            if self.count:
                self.batch.commit()
            self.batch = self.client.batch()
            self.count = 0

    def _thread_partitions(self, thread_id: str):
        """Yield ``(checkpoint_ns, partition_doc_ref)`` for every namespace of a thread.

        Partition docs are usually virtual (no fields), so ``list_documents`` is
        used rather than ``stream``.
        """
        prefix = f"{thread_id}_"
        for partition_doc in self.checkpoints_collection.list_documents():
            if partition_doc.id.startswith(prefix):
                yield partition_doc.id[len(prefix):], partition_doc

    def _delete_checkpoint_doc(self, batcher: "_Batcher", cp_ref) -> None:
        for w in cp_ref.collection("writes").list_documents():
            batcher.delete(w)
        batcher.delete(cp_ref)

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        """Copy every checkpoint and pending write of ``source_thread_id`` (all
        namespaces) to ``target_thread_id``.

        Checkpoint ids, parent ids, metadata and write ordering are preserved;
        only the thread segment of ``checkpoint_key`` is rewritten. The source
        thread is left untouched. A nonexistent source is a no-op. Writes go
        through batched commits.
        """
        if source_thread_id == target_thread_id:
            return
        batcher = self._Batcher(self.client, self._BATCH_LIMIT)
        for ns, src_partition in self._thread_partitions(source_thread_id):
            dst_collection = self._get_partition_collection(target_thread_id, ns)
            for cp_doc in src_partition.collection("checkpoints").stream():
                if not cp_doc.exists:
                    continue
                data = cp_doc.to_dict()
                parsed = _parse_firestore_checkpoint_key(data["checkpoint_key"])
                data["checkpoint_key"] = _make_firestore_checkpoint_key(
                    target_thread_id, parsed["checkpoint_ns"], parsed["checkpoint_id"]
                )
                dst_cp_ref = dst_collection.document(cp_doc.id)
                batcher.set(dst_cp_ref, data)
                for w_doc in cp_doc.reference.collection("writes").stream():
                    if not w_doc.exists:
                        continue
                    w_data = w_doc.to_dict()
                    wp = _parse_firestore_checkpoint_writes_key(w_data["checkpoint_key"])
                    w_data["checkpoint_key"] = _make_firestore_checkpoint_writes_key(
                        target_thread_id, wp["checkpoint_ns"], wp["checkpoint_id"],
                        wp["task_id"], wp["idx"],
                    )
                    batcher.set(dst_cp_ref.collection("writes").document(w_doc.id), w_data)
        batcher.flush()

    async def acopy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        await asyncio.get_running_loop().run_in_executor(
            None, self.copy_thread, source_thread_id, target_thread_id
        )

    def delete_for_runs(self, run_ids) -> None:
        """Delete every checkpoint (and its writes) whose ``metadata.run_id`` is
        in ``run_ids``, across all threads and namespaces.

        Implementation notes:

        * Matching relies on the top-level ``run_id`` field that ``put`` stores
          alongside the checkpoint. Documents written by older versions of this
          package lack that field and will not be found.
        * No collection-group query is used (it would require a manually
          created index). Instead every partition document is enumerated with
          ``list_documents`` and queried with ``where("run_id", "in", chunk)``
          (chunks of 30, the Firestore ``in`` limit), which uses the automatic
          single-field index. Cost is therefore O(number of threads).
        * Empty / unknown run ids are a no-op.
        """
        run_ids = [r for r in dict.fromkeys(run_ids) if r is not None]
        if not run_ids:
            return
        chunks = [run_ids[i:i + 30] for i in range(0, len(run_ids), 30)]
        batcher = self._Batcher(self.client, self._BATCH_LIMIT)
        for partition_doc in self.checkpoints_collection.list_documents():
            cp_collection = partition_doc.collection("checkpoints")
            for chunk in chunks:
                query = cp_collection.where(filter=FieldFilter("run_id", "in", chunk))
                for cp_doc in query.stream():
                    if cp_doc.exists:
                        self._delete_checkpoint_doc(batcher, cp_doc.reference)
        batcher.flush()

    async def adelete_for_runs(self, run_ids) -> None:
        await asyncio.get_running_loop().run_in_executor(
            None, self.delete_for_runs, list(run_ids)
        )

    def prune(self, thread_ids, *, strategy: str = "keep_latest") -> None:
        """Prune checkpoints for ``thread_ids``.

        ``strategy="keep_latest"`` keeps, per thread and per namespace
        partition, only the checkpoint with the greatest ``checkpoint_id`` (ids
        are time-ordered) together with its pending writes, deleting all
        others. ``strategy="delete"`` is equivalent to ``delete_thread``. Any
        other strategy raises ``ValueError``. Empty / unknown thread ids are a
        no-op.

        DeltaChannel caveat: this implementation is not delta-aware. If your
        graph uses ``DeltaChannel``, ``keep_latest`` may drop intermediate
        checkpoints/writes the surviving checkpoint needs for reconstruction.
        """
        if strategy not in ("keep_latest", "delete"):
            raise ValueError(f"Unknown prune strategy: {strategy!r}")
        for thread_id in thread_ids:
            if strategy == "delete":
                self.delete_thread(thread_id)
                continue
            batcher = self._Batcher(self.client, self._BATCH_LIMIT)
            for _ns, partition_doc in self._thread_partitions(thread_id):
                cp_refs = list(partition_doc.collection("checkpoints").list_documents())
                if len(cp_refs) <= 1:
                    continue
                latest = max(cp_refs, key=lambda r: r.id)
                for cp_ref in cp_refs:
                    if cp_ref.id != latest.id:
                        self._delete_checkpoint_doc(batcher, cp_ref)
            batcher.flush()

    async def aprune(self, thread_ids, *, strategy: str = "keep_latest") -> None:
        await asyncio.get_running_loop().run_in_executor(
            None, lambda: self.prune(list(thread_ids), strategy=strategy)
        )

    def _load_pending_writes(self, thread_id: str, checkpoint_ns: Optional[str] , checkpoint_id: str) -> List[PendingWrite]:
        
        partition_collection = self._get_partition_collection(thread_id, checkpoint_ns)
        writes_ref = partition_collection.document(checkpoint_id).collection("writes")

        # Stream all write documents under this checkpoint
        write_docs = [doc.to_dict() for doc in writes_ref.stream() if doc.exists]

        if not write_docs:
            return []

        # Parse checkpoint keys and sort by idx
        parsed_keys = [
            _parse_firestore_checkpoint_writes_key(w["checkpoint_key"]) for w in write_docs
        ]

        # Sort by idx (to maintain deterministic replay order)
        combined = sorted(zip(write_docs, parsed_keys), key=lambda x: x[1]["idx"])
        
        pending_writes = _load_writes(
            self.firestore_serde,
            {
                (parsed_key["task_id"], parsed_key["idx"]): key
                for key, parsed_key in combined
            },
        )
        return pending_writes
   
    def _get_checkpoint_key(self, thread_id: str, checkpoint_ns: str, checkpoint_id: Optional[str]) -> Optional[str]:
        if checkpoint_id:
            return _make_firestore_checkpoint_key(thread_id, checkpoint_ns, checkpoint_id)

        partition_collection = self._get_partition_collection(thread_id, checkpoint_ns)
        docs = (
            partition_collection
            .order_by("checkpoint_id", direction=firestore.Query.DESCENDING)
            .limit(1)
            .stream()
            )    
        
        if not docs:
            return None

        latest_doc = next(docs, None)
        if not latest_doc or not latest_doc.exists:
            return None

        data = latest_doc.to_dict()
        return data["checkpoint_key"]
    
    async def aget(self, config: RunnableConfig) -> Optional[Checkpoint]:
        if value := await self.aget_tuple(config):
            return value.checkpoint

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        return await asyncio.get_running_loop().run_in_executor(
            None, self.get_tuple, config
        )

    async def alist(self, config: Optional[RunnableConfig], *,
                    filter: Optional[Dict[str, Any]] = None,
                    before: Optional[RunnableConfig] = None,
                    limit: Optional[int] = None) -> AsyncIterator[CheckpointTuple]:
        loop = asyncio.get_running_loop()
        items = await loop.run_in_executor(
            None, lambda: list(self.list(config, filter=filter, before=before, limit=limit))
        )
        for item in items:
            yield item

    async def aput(
        self, config: RunnableConfig, checkpoint: Checkpoint, metadata: Optional[CheckpointMetadata] = None, new_versions: Optional[ChannelVersions] = None
    ) -> RunnableConfig:
        return await asyncio.get_running_loop().run_in_executor(
            None, self.put, config, checkpoint, metadata, new_versions
        )

    async def aput_writes(
        self, config: RunnableConfig, writes: List[Tuple[str, Any]], task_id: str, task_path: str = ""
    ) -> None:
        return await asyncio.get_running_loop().run_in_executor(
            None, self.put_writes, config, writes, task_id, task_path
        )
