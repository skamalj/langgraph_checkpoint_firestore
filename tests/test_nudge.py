"""The reducer hint: once per process, INFO level, silenceable, never when a reducer is passed."""
import logging

import pytest

from langgraph_checkpoint_firestore import _nudge


@pytest.fixture(autouse=True)
def reset(monkeypatch):
    monkeypatch.setattr(_nudge, "_shown", False)
    monkeypatch.delenv(_nudge.QUIET_ENV, raising=False)


def test_emitted_once_at_info(caplog):
    log = logging.getLogger("nudge-test")
    with caplog.at_level(logging.INFO, logger="nudge-test"):
        assert _nudge.nudge_unbounded_history(log) is True
        assert _nudge.nudge_unbounded_history(log) is False
    msgs = [r for r in caplog.records if "reducer=" in r.getMessage()]
    assert len(msgs) == 1
    assert msgs[0].levelno == logging.INFO
    assert _nudge.DOCS_URL in msgs[0].getMessage()
    assert "[reducer]" in msgs[0].getMessage()


def test_silenced_by_env(monkeypatch, caplog):
    monkeypatch.setenv(_nudge.QUIET_ENV, "1")
    log = logging.getLogger("nudge-test")
    with caplog.at_level(logging.INFO, logger="nudge-test"):
        assert _nudge.nudge_unbounded_history(log) is False
    assert not [r for r in caplog.records if "reducer=" in r.getMessage()]
