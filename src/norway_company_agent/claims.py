from __future__ import annotations

"""Deterministic observation -> evidence -> claim pipeline for external connectors.

This is the single canonical path by which any future connector turns raw
observations into evidence and publishable claims:

    raw observation
        -> provenance gate (``external_footprint.validate_observation``)
        -> identity gate (``identity_triangulation.resolve_candidate_identities``)
        -> organisation link check against the profile being enriched
        -> canonical evidence record
        -> deterministic claim assembly

Two rules drive the design. WRONG COMPANY is rejected before WEAK EVIDENCE: an
observation whose organisation number conflicts with the target profile is
``mismatched`` and produces neither evidence nor a claim, whatever its name,
domain, source or connector status look like. And blocked, ambiguous, failed,
mismatched or unverified input becomes an explicit rejection record in the
repository's existing state vocabulary, never a published factual claim.

The pipeline is a pure function of its inputs: it reads no clock, uses no random
identifier, no dictionary-order-dependent hashing, no LLM and no network. Repeated
runs over identical observations therefore produce byte-identical evidence ids and
claim ids, and ``diff_claims`` reports a change only when claim identity, value,
state or supporting evidence content actually changes.

Claims are assembled only from explicitly supplied structured facts. Arbitrary web
text and search snippets are stored as an evidence span and never parsed into
facts.
"""

import hashlib
import json
from typing import Any

from .evidence import evidence
from .external_footprint import validate_observation
from .identity_triangulation import (
    EXACT,
    canonical_identity_from_profile,
    resolve_candidate_identities,
)

EVIDENCE_AVAILABLE = "available"
CLAIM_PUBLISHED = "published"
CLAIM_CONFLICT = "conflict"
CLAIM_CLASSIFICATION = "external_connector_fact"

METHOD = "deterministic_observation_evidence_claim_v1"

# Rejection states follow the repository's existing vocabulary rather than a new one:
# connectors/base.AMBIGUOUS, identity_triangulation.MISMATCHED/UNVERIFIED, the
# evidence "blocked" status, and the connectors "failed" state.
STATE_MISMATCHED = "mismatched"
STATE_AMBIGUOUS = "ambiguous"
STATE_UNVERIFIED = "unverified"
STATE_BLOCKED = "blocked"
STATE_FAILED = "failed"

_BLOCKING_REASONS = frozenset(
    {
        "acquisition mode is not approved for publication",
        "source rights are not approved",
    }
)
_IDENTITY_REASONS = frozenset(
    {
        "exact legal entity is not verified",
        "missing exact-entity proof",
        "observation organisation number does not match the profile organisation number",
        "profile has no organisation number to bind evidence to",
    }
)


def _hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()[:24]


def canonical_serialization(value: Any) -> str:
    """Serialize a value the same way everywhere, so equivalent values collide."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def normalize_claim_value(value: Any) -> Any:
    """Reduce a supplied fact value to its canonical identity.

    Strings are whitespace-collapsed and casefolded, numbers become floats so that
    ``87`` and ``87.0`` never fight, and structures are compared by canonical JSON.
    The original supplied value is preserved separately on the claim.
    """
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return " ".join(value.split()).casefold()
    return canonical_serialization(value)


def evidence_id(organisation_number: str, source_url: str, content_sha256: str, value: Any) -> str:
    """Content-derived evidence id.

    Deliberately excludes ``retrieved_at`` so re-fetching unchanged content does
    not mint a new evidence identity, while any real content change does.
    """
    return _hash(
        {
            "organisation_number": organisation_number,
            "source_url": source_url,
            "content_sha256": content_sha256,
            "value": value,
        }
    )


def claim_id(organisation_number: str, field: str, normalized_value: Any, state: str) -> str:
    """Identity of a claim: company + field + normalized value + state.

    Evidence ids are intentionally excluded so corroboration by a second source
    enriches one claim instead of splitting it.
    """
    return _hash(
        {
            "organisation_number": organisation_number,
            "field": field,
            "normalized_value": normalized_value,
            "state": state,
        }
    )


def structured_facts(observation: dict[str, Any]) -> tuple[list[tuple[str, Any]], list[str]]:
    """Read explicitly supplied structured facts from an observation.

    ``facts`` is the canonical connector contract: a list of ``{"field", "value"}``
    objects. ``metrics`` is accepted as the equivalent for connectors that already
    emit a flat metric bag. Values that cannot be serialized are reported in the
    second return value instead of being silently dropped.
    """
    observation = observation if isinstance(observation, dict) else {}
    raw: list[tuple[str, Any]] = []
    if isinstance(observation.get("facts"), list):
        for item in observation["facts"]:
            if isinstance(item, dict) and item.get("field") not in (None, "") and "value" in item:
                raw.append((str(item["field"]), item["value"]))
    elif isinstance(observation.get("metrics"), dict):
        raw.extend((str(key), value) for key, value in observation["metrics"].items() if value is not None)

    supported: list[tuple[str, Any]] = []
    unsupported: list[str] = []
    for field, value in raw:
        try:
            json.dumps(value, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            if field not in unsupported:
                unsupported.append(field)
            continue
        supported.append((field, value))
    supported.sort(key=lambda item: (item[0], canonical_serialization(item[1])))
    return supported, unsupported


def _rejection_state(decision_status: str, reasons: list[str]) -> str:
    """Classify a rejection with the repository's existing state vocabulary.

    Identity beats rights: a candidate that is the wrong company, or whose link to
    the profile is unverified, is never reclassified as a mere rights block.
    """
    if decision_status == STATE_MISMATCHED:
        return STATE_MISMATCHED
    if decision_status == STATE_AMBIGUOUS:
        return STATE_AMBIGUOUS
    if decision_status == STATE_UNVERIFIED:
        return STATE_UNVERIFIED
    if any(reason in _IDENTITY_REASONS for reason in reasons):
        return STATE_UNVERIFIED
    if any(reason in _BLOCKING_REASONS for reason in reasons):
        return STATE_BLOCKED
    return STATE_FAILED


def _rejection(observation: dict[str, Any], profile: dict[str, Any], state: str, reasons: list[str]) -> dict[str, Any]:
    """Rejection record carrying identity and reasons but no evidence payload."""
    return {
        "id": observation.get("id"),
        "task_id": observation.get("task_id"),
        "connector": observation.get("connector"),
        "organisation_number": profile.get("organisation_number"),
        "platform": observation.get("platform"),
        "signal_type": observation.get("signal_type"),
        "state": state,
        "reasons": reasons,
        "method": METHOD,
    }


def observation_to_evidence(
    profile: dict[str, Any],
    observation: dict[str, Any],
    decision: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Apply both hard gates to one observation and emit canonical evidence.

    Returns ``(evidence, None)`` when the observation is publishable, otherwise
    ``(None, rejection)``. No third outcome exists: nothing is silently dropped.
    """
    profile = profile if isinstance(profile, dict) else {}
    observation = observation if isinstance(observation, dict) else {}
    target_organisation = str(profile.get("organisation_number") or "").strip()

    reasons: list[str] = []
    if not target_organisation.isdigit():
        reasons.append("profile has no organisation number to bind evidence to")

    declared = str(observation.get("organisation_number") or "").strip()
    if target_organisation.isdigit() and declared != target_organisation:
        reasons.append("observation organisation number does not match the profile organisation number")

    if decision is None:
        decision = resolve_candidate_identities(
            canonical_identity_from_profile(profile), [observation]
        )[0]
    if not decision.get("publishable"):
        reasons = [*decision.get("reasons", []), *reasons]
        return None, _rejection(observation, profile, _rejection_state(decision["status"], reasons), reasons)

    reasons.extend(validate_observation(observation))
    if reasons:
        return None, _rejection(observation, profile, _rejection_state(decision["status"], reasons), reasons)

    facts, unsupported = structured_facts(observation)
    source_url = str(observation.get("source_url") or "")
    content_hash = str(observation.get("content_sha256") or "")
    payload = [{"field": field, "value": value} for field, value in facts]

    record = evidence(
        str(observation.get("signal_type") or "external_observation"),
        EVIDENCE_AVAILABLE,
        str(observation.get("connector") or observation.get("platform") or "external_connector"),
        source_url,
        value=payload,
        retrieved_at=str(observation.get("retrieved_at")),
        content_sha256=content_hash,
        source_row_key=str(observation.get("id") or ""),
        note=str(observation.get("evidence_span") or "") or None,
    )
    record["evidence_id"] = evidence_id(target_organisation, source_url, content_hash, payload)
    record["span"] = observation.get("evidence_span")
    record["organisation_number"] = target_organisation
    record["acquisition_mode"] = observation.get("acquisition_mode")
    record["rights_status"] = observation.get("rights_status")
    record["identity"] = {
        "status": decision["status"],
        "method": decision.get("method"),
        "organisation_number": target_organisation,
        "candidate_organisation_number": decision.get("signals", {}).get("candidate_organisation_number"),
    }
    record["unsupported_fact_fields"] = unsupported
    return record, None


def build_evidence(profile: dict[str, Any], observations: list[dict[str, Any]]) -> dict[str, Any]:
    """Turn a batch of observations into canonical evidence and rejection records.

    The whole batch is triangulated together so that two distinct candidate
    identities inside one batch are downgraded to ``ambiguous`` exactly as the
    Phase 2 multi-candidate rule requires.
    """
    profile = profile if isinstance(profile, dict) else {}
    observations = list(observations or [])
    decisions = resolve_candidate_identities(canonical_identity_from_profile(profile), observations)
    evidence_items: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    for observation, decision in zip(observations, decisions):
        record, rejection = observation_to_evidence(profile, observation, decision)
        if record is None:
            rejections.append(rejection)
        else:
            evidence_items.append(record)

    by_id = {}
    for record in evidence_items:
        by_id.setdefault(record["evidence_id"], record)
    ordered = [by_id[key] for key in sorted(by_id)]
    rejections.sort(key=lambda item: (str(item.get("id") or ""), str(item.get("state") or "")))
    return {"evidence": ordered, "rejections": rejections}


def _claim_identity(evidence_record: dict[str, Any]) -> dict[str, Any]:
    return dict(evidence_record.get("identity") or {})


def assemble_claims(evidence_items: list[dict[str, Any]], organisation_number: str) -> list[dict[str, Any]]:
    """Assemble deterministic, evidence-backed claims from canonical evidence.

    Identical claims collapse onto one claim id, so the same observation or source
    encountered twice never duplicates. When two sources assert different values
    for one field the pipeline emits a single explicit ``conflict`` claim carrying
    every candidate value: it never picks a winner and never overwrites.

    Only evidence that is itself ``available`` and carries an ``exact`` identity is
    eligible. Blocked, rejected or unverified records passed in by mistake yield no
    claim rather than a claim without a verified provenance chain.
    """
    organisation = str(organisation_number or "")
    usable = [
        record
        for record in evidence_items
        if record.get("status") == EVIDENCE_AVAILABLE
        and (record.get("identity") or {}).get("status") == EXACT
    ]
    grouped: dict[str, dict[str, dict[str, Any]]] = {}
    for record in usable:
        for item in record.get("value") or []:
            if not isinstance(item, dict):
                continue
            field = str(item.get("field") or "")
            if not field:
                continue
            normalized = normalize_claim_value(item.get("value"))
            bucket = grouped.setdefault(field, {}).setdefault(
                canonical_serialization(normalized),
                {"normalized_value": normalized, "candidates": []},
            )
            bucket["candidates"].append({"evidence_id": record["evidence_id"], "value": item.get("value")})

    claims: list[dict[str, Any]] = []
    for field in sorted(grouped):
        variants = grouped[field]
        conflicting = len(variants) > 1
        supporting = sorted({c["evidence_id"] for v in variants.values() for c in v["candidates"]})
        records_by_id = {r["evidence_id"]: r for r in usable}

        if conflicting:
            normalized: Any = sorted(
                (bucket["normalized_value"] for bucket in variants.values()),
                key=canonical_serialization,
            )
            state = CLAIM_CONFLICT
            representative = min(
                (c for v in variants.values() for c in v["candidates"]),
                key=lambda c: (canonical_serialization(c["value"]), str(c["value"])),
            )["value"]
        else:
            only = variants[next(iter(variants))]
            normalized = only["normalized_value"]
            state = CLAIM_PUBLISHED
            representative = min(
                only["candidates"],
                key=lambda c: (str(c["evidence_id"]), canonical_serialization(c["value"]), str(c["value"])),
            )["value"]

        supporting_records = [records_by_id[key] for key in supporting if key in records_by_id]
        source_urls = sorted({str(r.get("source_url") or "") for r in supporting_records if r.get("source_url")})
        retrieved = sorted(str(r.get("retrieved_at")) for r in supporting_records if r.get("retrieved_at"))
        content_hashes = sorted({str(r.get("content_sha256") or "") for r in supporting_records if r.get("content_sha256")})
        identity = _claim_identity(supporting_records[0]) if supporting_records else {}

        claims.append(
            {
                "claim_id": claim_id(organisation, field, normalized, state),
                "claim": field,
                "field": field,
                "value": representative,
                "normalized_value": normalized,
                "state": state,
                "classification": CLAIM_CLASSIFICATION,
                "organisation_number": organisation,
                "evidence_ids": supporting,
                "source_urls": source_urls,
                "source_url": source_urls[0] if len(source_urls) == 1 else None,
                "retrieved_at": retrieved[0] if retrieved else None,
                "content_sha256": content_hashes,
                "identity": identity,
                "acquisition_mode": supporting_records[0].get("acquisition_mode") if supporting_records else None,
                "rights_status": supporting_records[0].get("rights_status") if supporting_records else None,
                "method": METHOD,
            }
        )

    claims.sort(key=lambda item: (item["claim_id"], item["field"]))
    return claims


def build_evidence_and_claims(
    profile: dict[str, Any],
    observations: list[dict[str, Any]],
) -> dict[str, Any]:
    """Run the full observation -> evidence -> claim path for one profile."""
    built = build_evidence(profile, observations)
    organisation = str((profile or {}).get("organisation_number") or "")
    claims = assemble_claims(built["evidence"], organisation)
    return {"evidence": built["evidence"], "claims": claims, "rejections": built["rejections"]}


def _claim_fingerprint(claim: dict[str, Any]) -> tuple[Any, ...]:
    """Everything about a claim that makes it true, excluding retrieval timestamps."""
    return (
        claim.get("state"),
        canonical_serialization(claim.get("normalized_value")),
        tuple(claim.get("evidence_ids") or []),
        tuple(claim.get("source_urls") or []),
        claim.get("classification"),
        claim.get("organisation_number"),
    )


def diff_claims(previous: list[dict[str, Any]], current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deterministic refresh comparison for claims.

    Compares claim identity, value, state, classification and supporting evidence
    only. ``retrieved_at`` is deliberately excluded so re-fetching unchanged source
    content reports no change, while any change in claim identity, value or
    supporting evidence is reported as a real change.
    """
    before = {str(item.get("claim_id") or ""): item for item in previous or []}
    after = {str(item.get("claim_id") or ""): item for item in current or []}
    changes: list[dict[str, Any]] = []
    for claim_key in sorted(set(before) | set(after)):
        old = before.get(claim_key)
        new = after.get(claim_key)
        if old is None:
            kind = "added"
        elif new is None:
            kind = "removed"
        elif _claim_fingerprint(old) != _claim_fingerprint(new):
            kind = "changed"
        else:
            continue
        reference = new or old or {}
        changes.append(
            {
                "change": kind,
                "claim_id": reference.get("claim_id"),
                "field": reference.get("field"),
                "old_value": (old or {}).get("value"),
                "new_value": (new or {}).get("value"),
                "state": reference.get("state"),
                "organisation_number": reference.get("organisation_number"),
                "source_urls": reference.get("source_urls"),
                "retrieved_at": reference.get("retrieved_at"),
                "content_sha256": reference.get("content_sha256"),
                "evidence_ids": reference.get("evidence_ids"),
            }
        )
    return changes
