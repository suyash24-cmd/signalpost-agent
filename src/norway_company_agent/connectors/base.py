from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..external_footprint import PUBLISHABLE_ACQUISITION_MODES

ALLOWED_ACQUISITION_MODES = frozenset(PUBLISHABLE_ACQUISITION_MODES)
ALLOWED_RIGHTS_STATUSES = frozenset({"approved"})

# Explicit failure states for connector outcomes.
FAILED = "failed"
NOT_AVAILABLE = "not_available"
AMBIGUOUS = "ambiguous"

# Rejection reasons understood by the executor.
FAILURE_UNKNOWN_CONNECTOR = "unknown_connector"
FAILURE_UNSUPPORTED_ACQUISITION_MODE = "unsupported_acquisition_mode"
FAILURE_UNSUPPORTED_TASK_TYPE = "unsupported_task_type"
FAILURE_RIGHTS_NOT_APPROVED = "rights_status_not_approved"
FAILURE_BUDGET_DENIED = "budget_denied"
FAILURE_CONNECTOR_FAILED = "connector_failed"
FAILURE_INVALID_TASK = "invalid_task"


class ConnectorResult:
    """Structured outcome of one connector execution.

    Success carries observations; failure carries an explicit state and reason.
    A task never vanishes: it yields observations or a structured failure.
    """

    __slots__ = ("task", "ok", "observations", "failure_detail", "requests_used", "cost")

    def __init__(
        self,
        task: dict[str, Any],
        *,
        ok: bool,
        observations: list[dict[str, Any]] | None = None,
        failure: dict[str, Any] | None = None,
        requests_used: int = 0,
        cost: float = 0.0,
    ) -> None:
        self.task = task
        self.ok = bool(ok)
        self.observations = list(observations or [])
        self.failure_detail = failure
        self.requests_used = max(0, int(requests_used))
        self.cost = max(0.0, float(cost))

    @classmethod
    def success(
        cls,
        task: dict[str, Any],
        observations: list[dict[str, Any]] = (),
        *,
        requests_used: int = 1,
        cost: float = 0.0,
    ) -> "ConnectorResult":
        return cls(task, ok=True, observations=list(observations), requests_used=requests_used, cost=cost)

    @classmethod
    def failed(
        cls,
        task: dict[str, Any],
        reason: str,
        *,
        state: str = FAILED,
        requests_used: int = 0,
        cost: float = 0.0,
    ) -> "ConnectorResult":
        return cls(task, ok=False, failure={"state": state, "reason": reason}, requests_used=requests_used, cost=cost)

    @property
    def failure_state(self) -> str | None:
        return None if self.ok else str((self.failure_detail or {}).get("state") or FAILED)


class BaseConnector(ABC):
    name: str = ""
    acquisition_mode: str = ""
    rights_status: str = ""
    supported_task_types: frozenset[str] = frozenset()
    cost_per_request: float = 0.0

    @abstractmethod
    def estimate_requests(self, task: dict[str, Any]) -> int:
        """Expected number of outbound requests for this task."""

    @abstractmethod
    def execute(self, task: dict[str, Any], budget: Any, *, now: str) -> ConnectorResult:
        """Acquire the signal and return observations or an explicit failure.

        Any outbound request beyond the executor's pre-reserved estimate must be
        reserved through the budget; a denial means the connector must abort with
        an explicit failure result.
        """

    def supports(self, task: dict[str, Any]) -> bool:
        if not self.supported_task_types:
            return True
        return str(task.get("purpose") or "") in self.supported_task_types