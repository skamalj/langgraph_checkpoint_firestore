"""LangGraph's official checkpointer conformance suite (langgraph-checkpoint-conformance) against FirestoreSaver.

Requires GCP ADC; uses a throwaway collection in GCP_PROJECT_ID (default gcdeveloper-new).
"""
import os
import uuid

import pytest
from langgraph.checkpoint.conformance import checkpointer_test
from langgraph.checkpoint.conformance.report import ProgressCallbacks
from langgraph.checkpoint.conformance.validate import validate

from langgraph_checkpoint_firestore import FirestoreSaver

PROJECT = os.environ.get("GCP_PROJECT_ID", "gcdeveloper-new")
_COLLECTION = f"conformance_{uuid.uuid4().hex[:8]}"


@checkpointer_test(name="FirestoreSaver")
async def _saver():
    yield FirestoreSaver(PROJECT, _COLLECTION)


@pytest.mark.asyncio
async def test_official_conformance_base_capabilities():
    report = await validate(_saver, progress=ProgressCallbacks.quiet())
    failures = {cap: r.failures for cap, r in report.results.items() if r.failures}
    assert report.passed_all_base(), failures
    assert report.conformance_level() == "FULL"
