from __future__ import annotations

"""Minimal request/cost budget shared by every external connector request.

The budget is intentionally simple: there is no distributed rate limiting, just a
hard per-run ceiling on outbound requests and estimated/actual cost. Every
external connector request must be reserved through this layer before it is
allowed to count.

Reserved requests drive the cap decisions; actual requests/cost are recorded
separately after execution when the connector reports them, so the two are never
added together.
"""

import time
from collections import Counter
from typing import Any, Callable


class RequestBudget:
    def __init__(
        self,
        max_requests: int,
        *,
        max_cost: float | None = None,
        default_cost_per_request: float = 0.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_requests = max(0, int(max_requests))
        self.max_cost = None if max_cost is None else float(max_cost)
        self.default_cost_per_request = float(default_cost_per_request)
        self._clock = clock
        self._total_requests = 0
        self._requests_per_connector: Counter[str] = Counter()
        self._actual_requests = 0
        self._actual_requests_per_connector: Counter[str] = Counter()
        self._estimated_cost = 0.0
        self._actual_cost = 0.0
        self._denied_requests = 0
        self._denied_cost = 0.0
        self._started_at = clock()

    @property
    def total_requests(self) -> int:
        """Outbound requests reserved so far (drives the request cap)."""
        return self._total_requests

    @property
    def requests_per_connector(self) -> dict[str, int]:
        return dict(sorted(self._requests_per_connector.items()))

    @property
    def actual_requests(self) -> int:
        return self._actual_requests

    @property
    def actual_requests_per_connector(self) -> dict[str, int]:
        return dict(sorted(self._actual_requests_per_connector.items()))

    @property
    def estimated_cost(self) -> float:
        return round(self._estimated_cost, 6)

    @property
    def actual_cost(self) -> float:
        return round(self._actual_cost, 6)

    @property
    def denied_requests(self) -> int:
        return self._denied_requests

    @property
    def denied_cost(self) -> float:
        return round(self._denied_cost, 6)

    @property
    def remaining_requests(self) -> int:
        return max(0, self.max_requests - self._total_requests)

    @property
    def elapsed_seconds(self) -> float:
        return round(max(0.0, self._clock() - self._started_at), 6)

    def can_spend(self, requests: int, cost: float) -> bool:
        requests = max(1, int(requests))
        cost = max(0.0, float(cost))
        within_requests = (self._total_requests + requests) <= self.max_requests
        within_cost = self.max_cost is None or (self._estimated_cost + cost) <= self.max_cost + 1e-9
        return within_requests and within_cost

    def reserve(self, connector: str, requests: int = 1, *, cost: float | None = None) -> bool:
        """Consume budget for an outbound request; the only sanctioned spend path.

        Returns False and records the denial when the request or cost cap is
        exceeded. Connectors must abort when a reservation is denied.
        """
        requests = max(1, int(requests))
        cost = self.default_cost_per_request * requests if cost is None else float(cost)
        cost = max(0.0, cost)
        if not self.can_spend(requests, cost):
            self._denied_requests += requests
            self._denied_cost += cost
            return False
        self._total_requests += requests
        self._requests_per_connector[connector] += requests
        self._estimated_cost += cost
        return True

    def record_actual(self, connector: str, requests: int = 0, cost: float = 0.0) -> None:
        """Record observed spend after execution, when the connector reports it."""
        requests = max(0, int(requests))
        cost = max(0.0, float(cost))
        self._actual_requests += requests
        self._actual_requests_per_connector[connector] += requests
        self._actual_cost += cost

    def summary(self) -> dict[str, Any]:
        return {
            "max_requests": self.max_requests,
            "max_cost": self.max_cost,
            "total_requests": self._total_requests,
            "requests_per_connector": self.requests_per_connector,
            "actual_requests": self._actual_requests,
            "actual_requests_per_connector": self.actual_requests_per_connector,
            "estimated_cost": self.estimated_cost,
            "actual_cost": self.actual_cost,
            "denied_requests": self._denied_requests,
            "denied_cost": self.denied_cost,
            "remaining_requests": self.remaining_requests,
            "elapsed_seconds": self.elapsed_seconds,
        }