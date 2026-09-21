"""One-time, silenceable hint emitted when a saver is built without a reducer.

The checkpointer works fine without one, but message history then grows
without bound on every thread. The hint points at the two-line fix and at the
long-term-memory hook it unlocks. It is logged once per process at INFO on the
package logger and never raised. Silence it with AGENTSTATE_QUIET=1 or by
passing reducer=.
"""
import logging
import os

DOCS_URL = "https://skamalj.github.io/agentstate-reducer/reducer/long-term-memory/"
QUIET_ENV = "AGENTSTATE_QUIET"

_shown = False


def nudge_unbounded_history(logger: logging.Logger, saver_name: str = "FirestoreSaver") -> bool:
    """Log the hint once per process. Returns True if it was emitted."""
    global _shown
    if _shown or os.getenv(QUIET_ENV):
        return False
    _shown = True
    logger.info(
        "%s was built without reducer=, so message history grows without bound on every thread. "
        "pip install 'langgraph-checkpoint-firestore[reducer]' and pass "
        "reducer=MessageReducer(config=ReducerConfig(max_messages=20)) to keep it bounded; "
        "add on_prune=[...] to turn pruned turns into long-term memory. %s (silence: %s=1)",
        saver_name, DOCS_URL, QUIET_ENV,
    )
    return True
