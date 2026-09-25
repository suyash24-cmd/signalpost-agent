from __future__ import annotations

"""Deterministic executor for the output of ``plan_external_tasks(profile)``.

The executor owns the publishability gate. No observation may be emitted unless it
independently passes the existing ``validate_observation`` gate, which requires
an explicitly verified exact entity, an allowed acquisition mode, and an approved
rights status. The executor never stamps ``exact_entity`` itself, so external
observations can never bypass identity verification.

Before that gate runs, every observation is independently triangulated against the
target company identity carried by the task. A connector cannot assert its way into
publication: an observation whose identity resolves to anything other than ``exact``
is rejected with the decision and its reasons, whatever the connector claimed. A
candidate that declares a foreign organisation number is ``mismatched`` and can never
be published.

Safety invariant: ABSTAIN > WRONG COMPANY. An unknown connector or unknown
acquisition mode is an explicit failure (FAILED / NOT_AVAILABLE), never silent
success, and an ambiguous identity is rejected as NOT PUBLISHABLE, never best
effort. The executor makes no LLM calls and is deterministic for a fixed ``now``.

Every connector invocation happens inside one containment boundary, so an
exception raised by ``supports()``, ``estimate_requests()``, the estimate
coercion, the task's budget operations or ``execute()`` becomes a structured
per-task failure record naming the stage it happened in, never a batch-ending
traceback. Each planned task therefore always yields observations or an explicit
failure, which keeps one terminal envelope per input.
"""

import json
from typing import Any

from ..evidence import utc_now
from ..external_footprint import publishable_observation, validate_observation
from ..external_tasks import plan_external_tasks
from ..identity_triangulation import (
    canonical_identity_from_task,
    resolve_candidate_identities,
)
from .base import (
    ALLOWED_ACQUISITION_MODES,
    ALLOWED_RIGHTS_STATUSES,
    ConnectorResult,
    FAILURE_BUDGET_DENIED,
    FAILURE_CONNECTOR_FAILED,
    FAILURE_INVALID_TASK,
    FAILURE_RIGHTS_NOT_APPROVED,
    FAILURE_UNKNOWN_CONNECTOR,
    FAILURE_UNSUPPORTED_ACQUISITION_MODE,
    FAILURE_UNSUPPORTED_TASK_TYPE,
    FAILED,
    NOT_AVAILABLE,
)
from .budget import RequestBudget
from .registry import ConnectorRegistry

STAGE_TASK_VALIDATION = "task_validation"
STAGE_REGISTRY_LOOKUP = "registry_lookup"
STAGE_ACQUISITION_GATE = "acquisition_mode_gate"
STAGE_RIGHTS_GATE = "rights_gate"
STAGE_SUPPORT_CHECK = "support_check"
STAGE_REQUEST_ESTIMATION = "request_estimation"
STAGE_ESTIMATE_COERCION = "estimate_coercion"
STAGE_BUDGET_RESERVATION = "budget_reservation"
STAGE_EXECUTION = "execution"
STAGE_BUDGET_ACCOUNTING = "budget_accounting"


def _task_key(task: Any) -> str:
    if isinstance(task, dict):
        task_id = task.get("task_id")
        if task_id:
            return str(task_id)
    try:
        return json.dumps(task, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        return repr(task)


def _failure_record(task: Any, connector: str | None, reason: str, state: str, stage: str) -> dict[str, Any]:
    if isinstance(task, dict):
        task_id = task.get("task_id")
        organisation_number = task.get("organisation_number")
    else:
        task_id = _task_key(task)
        organisation_number = None
    return {
        "task_id": task_id,
        "connector": connector,
        "organisation_number": organisation_number,
        "state": state,
        "reason": reason,
        "stage": stage,
    }


def _observation_item(observation: dict[str, Any], task: dict[str, Any], connector: Any, now: str) -> dict[str, Any]:
    item = {**observation}
    item["task_id"] = task.get("task_id")
    item["connector"] = connector.name
    item["organisation_number"] = task.get("organisation_number")
    item.setdefault("retrieved_at", now)
    return item


def _gate_observations(
    observations: list[dict[str, Any]],
    task: dict[str, Any],
    connector: Any,
    now: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    survivors: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for observation in observations:
        declared_org = observation.get("organisation_number")
        if declared_org is not None and str(declared_org) != str(task.get("organisation_number") or ""):
            rejected.append(
                {
                    "id": observation.get("id"),
                    "task_id": task.get("task_id"),
                    "connector": connector.name,
                    "organisation_number": task.get("organisation_number"),
                    "platform": observation.get("platform"),
                    "signal_type": observation.get("signal_type"),
                    "reasons": ["organisation number does not match the planned task"],
                }
            )
            continue
        survivors.append((observation, _observation_item(observation, task, connector, now)))

    decisions = resolve_candidate_identities(
        canonical_identity_from_task(task), [observation for observation, _ in survivors]
    )

    for (_, item), decision in zip(survivors, decisions):
        if decision["publishable"] and publishable_observation(item):
            accepted.append(item)
            continue
        reasons: list[str] = []
        if not decision["publishable"]:
            reasons = [
                f"identity triangulation resolved the candidate as {decision['status']}",
                *decision["reasons"],
            ]
        rejected.append(
            {
                "id": item.get("id"),
                "task_id": item.get("task_id"),
                "connector": connector.name,
                "organisation_number": task.get("organisation_number"),
                "platform": item.get("platform"),
                "signal_type": item.get("signal_type"),
                "reasons": [*reasons, *validate_observation(item)],
            }
        )
    return accepted, rejected


def run_external_tasks(
    tasks: list[dict[str, Any]],
    registry: ConnectorRegistry | None = None,
    budget: RequestBudget | None = None,
    *,
    now: str | None = None,
) -> dict[str, Any]:
    """Execute planned external tasks through the connector registry.

    Every planned task yields either accepted observations or an explicit
    failure/rejection record; nothing is silently dropped. Tasks are processed in
    deterministic order and the function makes no LLM calls.
    """
    registry = registry if registry is not None else ConnectorRegistry()
    budget = budget if budget is not None else RequestBudget(max_requests=0)
    now = now or utc_now()

    observations: list[dict[str, Any]] = []
    rejected_observations: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    planned = 0
    completed = 0
    failed = 0

    ordered = sorted(tasks, key=_task_key)
    for task in ordered:
        planned += 1
        if not isinstance(task, dict) or not str(task.get("connector") or "").strip():
            failures.append(_failure_record(task, None, FAILURE_INVALID_TASK, FAILED, STAGE_TASK_VALIDATION))
            failed += 1
            continue

        connector_name = str(task["connector"])
        connector = registry.get(connector_name)
        if connector is None:
            failures.append(_failure_record(task, connector_name, FAILURE_UNKNOWN_CONNECTOR, NOT_AVAILABLE, STAGE_REGISTRY_LOOKUP))
            failed += 1
            continue
        stage = STAGE_ACQUISITION_GATE
        result_stage = stage
        try:
            if str(getattr(connector, "acquisition_mode", "") or "") not in ALLOWED_ACQUISITION_MODES:
                failures.append(_failure_record(task, connector_name, FAILURE_UNSUPPORTED_ACQUISITION_MODE, NOT_AVAILABLE, STAGE_ACQUISITION_GATE))
                failed += 1
                continue

            stage = STAGE_RIGHTS_GATE
            if str(getattr(connector, "rights_status", "") or "") not in ALLOWED_RIGHTS_STATUSES:
                failures.append(_failure_record(task, connector_name, FAILURE_RIGHTS_NOT_APPROVED, NOT_AVAILABLE, STAGE_RIGHTS_GATE))
                failed += 1
                continue

            stage = STAGE_SUPPORT_CHECK
            if not connector.supports(task):
                failures.append(_failure_record(task, connector_name, FAILURE_UNSUPPORTED_TASK_TYPE, NOT_AVAILABLE, STAGE_SUPPORT_CHECK))
                failed += 1
                continue

            stage = STAGE_REQUEST_ESTIMATION
            raw_estimate = connector.estimate_requests(task)

            stage = STAGE_ESTIMATE_COERCION
            estimate = max(1, int(raw_estimate))

            stage = STAGE_BUDGET_RESERVATION
            reserved = budget.reserve(connector_name, estimate, cost=estimate * float(getattr(connector, "cost_per_request", 0.0) or 0.0))
            if not reserved:
                failures.append(_failure_record(task, connector_name, FAILURE_BUDGET_DENIED, FAILED, STAGE_BUDGET_RESERVATION))
                failed += 1
                continue

            stage = STAGE_EXECUTION
            result = connector.execute(task, budget, now=now)
            result_stage = STAGE_EXECUTION

            stage = STAGE_BUDGET_ACCOUNTING
            budget.record_actual(connector_name, result.requests_used or 0, result.cost or 0.0)
        except Exception as exc:  # a failing connector must never take down the batch
            result = ConnectorResult.failed(task, f"{type(exc).__name__}: {exc}", state=FAILED)
            result_stage = stage

        if not result.ok:
            state = result.failure_state or FAILED
            detail = result.failure_detail or {}
            failures.append(
                {
                    "task_id": task.get("task_id"),
                    "connector": connector_name,
                    "organisation_number": task.get("organisation_number"),
                    "state": state,
                    "reason": detail.get("reason") or FAILURE_CONNECTOR_FAILED,
                    "stage": result_stage,
                }
            )
            failed += 1
            continue

        accepted, rejected = _gate_observations(result.observations, task, connector, now)
        observations.extend(accepted)
        rejected_observations.extend(rejected)
        completed += 1

    observations.sort(key=lambda item: str(item.get("id") or ""))
    rejected_observations.sort(key=lambda item: str(item.get("id") or ""))
    failures.sort(key=lambda item: str(item.get("task_id") or "") + str(item.get("reason") or ""))

    return {
        "planned_tasks": planned,
        "completed_tasks": completed,
        "failed_tasks": failed,
        "observations": observations,
        "rejected_observations": rejected_observations,
        "failures": failures,
        "budget": budget.summary(),
    }


def run_external_step(profile: dict[str, Any], registry: ConnectorRegistry | None = None, budget: RequestBudget | None = None, *, now: str | None = None) -> dict[str, Any]:
    """Plan and execute external tasks for one profile.

    This is the unit the production pipeline calls after the website identity
    gate. Only publishable observations are attached to the profile; unverified
    observations are returned as rejection records (counts, ids and reasons) so
    nothing is silently dropped, but their evidence payload never reaches profile
    or terminal-envelope output.
    """
    tasks = plan_external_tasks(profile)
    result = run_external_tasks(tasks, registry=registry, budget=budget, now=now)
    step = {
        "planned_tasks": result["planned_tasks"],
        "completed_tasks": result["completed_tasks"],
        "failed_tasks": result["failed_tasks"],
        "observations": result["observations"],
        "rejected_count": len(result["rejected_observations"]),
        "rejections": result["rejected_observations"],
        "failures": result["failures"],
        "budget": result["budget"],
    }
    return step