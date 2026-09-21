"""LangGraph's official checkpointer conformance suite (langgraph-checkpoint-conformance) against FirestoreSaver.

Requires GCP ADC; uses a throwaway collection in GCP_PROJECT_ID (default gcdeveloper-new).
"""
import os
import uuid

import pytest
from langgraph.checkpoint.conformance import checkpointer_test
from langgraph.checkpoint.conformance.capabilities import EXTENDED_CAPABILITIES
from langgraph.checkpoint.conformance.report import ProgressCallbacks
from langgraph.checkpoint.conformance.validate import validate

from langgraph_checkpoint_firestore import FirestoreSaver

PROJECT = os.environ.get("GCP_PROJECT_ID", "gcdeveloper-new")
_COLLECTION = f"conformance_{uuid.uuid4().hex[:8]}"


@checkpointer_test(name="FirestoreSaver")
async def _saver():
    yield FirestoreSaver(PROJECT, _COLLECTION)


@pytest.mark.asyncio
async def test_official_conformance_all_capabilities():
    report = await validate(_saver, progress=ProgressCallbacks.quiet())
    report.print_report()
    failures = {cap: r.failures for cap, r in report.results.items() if r.failures}
    assert report.passed_all_base(), failures
    # Every extended capability must be detected (implemented) and pass.
    for cap in EXTENDED_CAPABILITIES:
        result = report.results.get(cap.value)
        assert result is not None and result.detected, f"{cap.value} not implemented"
        assert result.passed is True, {cap.value: result.failures}
    assert report.passed_all(), failures
    assert report.conformance_level() == "FULL"
