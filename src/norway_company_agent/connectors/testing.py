from __future__ import annotations

"""Deterministic mock connector used only for testing the execution architecture.

It is intentionally isolated from every production code path:

* it is never registered in ``registry.default_registry()``,
* the production pipeline never imports this module, and
* ``mock_registry()`` refuses to build a registry unless
  ``SIGNALPOST_ENABLE_TEST_CONNECTORS=1`` is set.

The mock performs no network I/O and makes no LLM calls. Its behaviour for a task
is selected by the deterministic ``mock_mode`` task field so tests can exercise
success, identity rejection, ambiguous identity, and hard connector failure.
"""

import os

from .base import FAILED, NOT_AVAILABLE, BaseConnector, ConnectorResult
from .registry import ConnectorRegistry


def _observation_id(task_id: str, index: int) -> str:
    return str(task_id or "")[:12] + f"-mock-{index:04d}-" + "0" * 7


class MockConnector(BaseConnector):
    """Configurable deterministic connector for executor tests."""

    name = "mock_places"
    acquisition_mode = "official_api"
    rights_status = "approved"
    supported_task_types = {"resolve_places_and_public_rating", "discover_active_jobs", "discover_independent_mentions"}
    cost_per_request = 0.0

    def __init__(
        self,
        *,
        name: str = "mock_places",
        acquisition_mode: str = "official_api",
        rights_status: str = "approved",
        supported_task_types=None,
        cost_per_request: float = 0.0,
        default_mode: str = "success",
    ) -> None:
        self.name = name
        self.acquisition_mode = acquisition_mode
        self.rights_status = rights_status
        if supported_task_types is not None:
            self.supported_task_types = frozenset(supported_task_types)
        self.cost_per_request = cost_per_request
        self.default_mode = default_mode

    def estimate_requests(self, task: dict) -> int:
        return max(1, int(task.get("mock_requests") or 1))

    def execute(self, task: dict, budget: object, *, now: str) -> ConnectorResult:
        mode = str(task.get("mock_mode") or self.default_mode)
        requests_used = self.estimate_requests(task)
        if mode == "fail":
            return ConnectorResult.failed(task, "mock connector failed on record", state=FAILED)
        if mode == "not_available":
            return ConnectorResult.failed(task, "mock source reports no record", state=NOT_AVAILABLE)
        if mode == "raise_error":
            raise RuntimeError("mock connector raised")

        observation = {
            "id": _observation_id(task.get("task_id"), 0),
            "organisation_number": task.get("organisation_number"),
            "platform": "google_places",
            "signal_type": "place_summary",
            "source_url": "https://example.test/places/123456789",
            "content_sha256": "a" * 64,
            "identity_proof": [{"type": "domain_match", "value": "example.test"}],
            "acquisition_mode": self.acquisition_mode,
            "rights_status": self.rights_status,
            "metrics": {"rating": 4.5, "review_count": 87},
            "evidence_span": "4.5/5 from 87 verified reviews",
        }
        if mode == "unverified":
            observation.pop("exact_entity", None)
            observation.pop("identity_proof", None)
        elif mode == "ambiguous":
            observation["exact_entity"] = False
            observation["identity_proof"] = []
        elif mode == "unknown_acquisition":
            observation["exact_entity"] = True
            observation["acquisition_mode"] = "web_scrape_default"
        elif mode == "unapproved_rights":
            observation["exact_entity"] = True
            observation["rights_status"] = "review_required"
        elif mode == "cross_organisation":
            observation["exact_entity"] = True
            observation["organisation_number"] = "111222333"
        else:
            observation["exact_entity"] = True
        return ConnectorResult.success(task, [observation], requests_used=requests_used, cost=requests_used * self.cost_per_request)


def mock_task(
    *,
    organisation_number: str = "923609016",
    company_name: str = "Example AS",
    municipality: str = "OSLO",
    connector: str = "mock_places",
    purpose: str = "resolve_places_and_public_rating",
    mode: str | None = None,
    extra: dict | None = None,
) -> dict:
    """Build a deterministic planned task for the mock connector."""
    import hashlib
    import json

    payload = {
        "organisation_number": organisation_number,
        "company_name": company_name,
        "municipality": municipality,
        "connector": connector,
        "purpose": purpose,
        "state": "pending",
    }
    if extra:
        payload.update(extra)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    task = {"task_id": hashlib.sha256(encoded).hexdigest()[:24], **payload}
    if mode:
        task["mock_mode"] = mode
    return task


def mock_registry(*connectors) -> ConnectorRegistry:
    """Build a registry of mock connectors for tests.

    Guarded so the mock can never be wired into a production run by accident.
    """
    if os.environ.get("SIGNALPOST_ENABLE_TEST_CONNECTORS") not in {"1", "true", "True"}:
        raise RuntimeError("mock/test connectors are disabled; set SIGNALPOST_ENABLE_TEST_CONNECTORS=1")
    return ConnectorRegistry(list(connectors) if connectors else [MockConnector()])