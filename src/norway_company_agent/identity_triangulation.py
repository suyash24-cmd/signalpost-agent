"""Deterministic identity triangulation between a target company and an external candidate.

ABSTAIN > WRONG COMPANY. A candidate external entity becomes publishable only when it
is tied to the exact target Norwegian organisation by deterministic evidence. This
module reuses the repository's existing normalization and matching semantics rather
than introducing a new scoring framework:

* name comparison uses ``identity._tokens`` (Norwegian character folding, NFKD,
  casefolding and legal-form removal) and the subset rule already used by
  ``identity.assess_website_identity`` and by the places candidate gate;
* domain comparison uses ``external_footprint._host`` and the same-site rule already
  used by the places candidate gate: identical host, or a subdomain of the target
  host. ``example.no`` therefore never matches ``example-other.no``;
* address comparison mirrors the existing registry street/postcode/poststed
  corroboration and is supporting evidence only.

A candidate that declares a different organisation number than the target is
``mismatched`` and can never be publishable, no matter how similar its name, domain
or address look. Address alone never establishes exact identity, and a fuzzy or
partial name match never establishes identity on its own.

No LLM is used and every decision is a pure function of its inputs.
"""

from __future__ import annotations

import re
import urllib.parse
from typing import Any

from .external_footprint import _host
from .identity import _tokens

EXACT = "exact"
AMBIGUOUS = "ambiguous"
MISMATCHED = "mismatched"
UNVERIFIED = "unverified"

IDENTITY_DECISIONS = (EXACT, AMBIGUOUS, MISMATCHED, UNVERIFIED)

METHOD = "deterministic_identity_triangulation_v1"


def normalized_domain(value: Any) -> str:
    """Return the comparable host for a URL, domain or hostname.

    Scheme, ``www.`` prefix, case, port, userinfo, path, query and a trailing dot
    are all normalized away, reusing ``external_footprint._host``. No public-suffix
    lookup is performed, so this stays deterministic and offline.
    """
    raw = str(value or "").strip().rstrip(".")
    if not raw:
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    try:
        parsed = urllib.parse.urlparse(raw)
    except ValueError:
        return ""
    return _host("https://" + (parsed.hostname or "").rstrip("."))


def same_site(candidate_domain: Any, target_domain: Any) -> bool:
    """Report whether two domains denote the same site under the existing places rule.

    Only an identical host or a subdomain of the target host qualifies. A deceptive
    host such as ``notexample.no`` or ``example-other.no`` is never the same site as
    ``example.no`` because the comparison is label-boundary exact.
    """
    candidate = normalized_domain(candidate_domain)
    target = normalized_domain(target_domain)
    if not candidate or not target:
        return False
    return candidate == target or candidate.endswith("." + target)


def name_match(target_name: Any, candidate_name: Any) -> tuple[bool, float]:
    """Compare legal names with the repository's conservative token-subset rule."""
    core = set(_tokens(target_name))
    other = set(_tokens(candidate_name))
    if not core or not other:
        return False, 0.0
    if core.issubset(other):
        return True, 1.0
    return False, len(core & other) / len(core)


def _address_parts(value: Any) -> dict[str, str]:
    if isinstance(value, str):
        return {"street": value, "postcode": "", "city": "", "municipality": ""}
    if not isinstance(value, dict):
        return {"street": "", "postcode": "", "city": "", "municipality": ""}
    return {
        "street": str(value.get("adresse") or value.get("street") or value.get("forretningsadresse.adresse") or ""),
        "postcode": str(value.get("postnummer") or value.get("postcode") or value.get("forretningsadresse.postnummer") or ""),
        "city": str(value.get("poststed") or value.get("city") or value.get("forretningsadresse.poststed") or ""),
        "municipality": str(value.get("kommune") or value.get("municipality") or ""),
    }


def address_support(target_address: Any, candidate_address: Any) -> dict[str, bool]:
    """Compare registry address with a candidate address as supporting evidence only."""
    target = _address_parts(target_address)
    candidate = _address_parts(candidate_address)
    candidate_tokens = set(
        _tokens(" ".join([candidate["street"], candidate["postcode"], candidate["city"]]))
    )
    street_tokens = set(_tokens(target["street"]))
    city_tokens = set(_tokens(target["city"]))
    postcode = re.sub(r"\D", "", target["postcode"])
    street_match = bool(street_tokens and street_tokens.issubset(candidate_tokens))
    postcode_match = bool(postcode and postcode in candidate_tokens)
    city_match = bool(city_tokens and city_tokens.issubset(candidate_tokens))
    return {
        "street_match": street_match,
        "postcode_match": postcode_match,
        "city_match": city_match,
        "supported": bool((street_match and postcode_match) or (postcode_match and city_match)),
    }


def canonical_identity_from_profile(profile: dict[str, Any]) -> dict[str, Any]:
    """Read the canonical target identity out of an existing company profile.

    The registered website domain is taken from the official BRREG record first and
    otherwise from the crawled website, but only when the existing website identity
    gate already considers that site publishable. A quarantined or non-exact website
    can therefore never become an identity anchor.
    """
    profile = profile if isinstance(profile, dict) else {}
    evidence = profile.get("evidence") or {}
    registry = (evidence.get("registry_live") or {}).get("value") or (evidence.get("registry") or {}).get("value") or {}
    website = evidence.get("website") or {}
    web_value = website.get("value") or {}
    assessment = web_value.get("identity_assessment") or {}
    domain = normalized_domain(registry.get("website"))
    if not domain and website.get("status") == "available" and assessment.get("publishable"):
        domain = normalized_domain(web_value.get("final_url") or website.get("source_url"))
    address = registry.get("business_address") or registry.get("postal_address") or {}
    parts = _address_parts(address)
    return {
        "organisation_number": str(profile.get("organisation_number") or "").strip(),
        "name": str(profile.get("name") or ""),
        "name_tokens": sorted(set(_tokens(profile.get("name")))),
        "domain": domain,
        "address": parts,
        "municipality": str(parts.get("municipality") or profile.get("municipality") or ""),
    }


def canonical_identity_from_task(task: dict[str, Any]) -> dict[str, Any]:
    """Read the canonical target identity carried by a planned external task.

    Falls back to the task's own organisation number, company name and municipality
    so a task that carries no explicit target identity still resolves deterministically.
    """
    task = task if isinstance(task, dict) else {}
    supplied = task.get("target_identity")
    supplied = supplied if isinstance(supplied, dict) else {}
    address = supplied.get("address") if isinstance(supplied.get("address"), (dict, str)) else {}
    return {
        "organisation_number": str(supplied.get("organisation_number") or task.get("organisation_number") or "").strip(),
        "name": str(supplied.get("name") or task.get("company_name") or ""),
        "name_tokens": sorted(set(_tokens(supplied.get("name") or task.get("company_name")))),
        "domain": normalized_domain(supplied.get("domain") or supplied.get("website")),
        "address": _address_parts(address),
        "municipality": str(supplied.get("municipality") or task.get("municipality") or ""),
    }


def _candidate_signals(candidate: dict[str, Any]) -> dict[str, Any]:
    candidate = candidate if isinstance(candidate, dict) else {}
    metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    domain_source = candidate.get("domain") or candidate.get("website") or candidate.get("source_url") or ""
    if not domain_source:
        for proof in candidate.get("identity_proof") or []:
            if isinstance(proof, dict) and "domain" in str(proof.get("type") or "").casefold():
                domain_source = proof.get("value")
                break
    return {
        "organisation_number": str(candidate.get("organisation_number") or "").strip(),
        "name": str(candidate.get("name") or candidate.get("title") or metrics.get("title") or ""),
        "domain": normalized_domain(domain_source),
        "address": candidate.get("address") or metrics.get("address") or "",
        "municipality": str(candidate.get("municipality") or metrics.get("municipality") or ""),
    }


def triangulate_identity(target: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    """Decide whether one candidate is the exact target organisation.

    Returns a decision with the repository's identity vocabulary: ``exact`` (may
    proceed to the publication gate), or ``ambiguous`` / ``mismatched`` / ``unverified``
    (never publishable).
    """
    target = target if isinstance(target, dict) else {}
    signals = _candidate_signals(candidate)
    target_org = str(target.get("organisation_number") or "").strip()
    candidate_org = signals["organisation_number"]
    name_exact, name_score = name_match(target.get("name"), signals["name"])
    domain_match = same_site(signals["domain"], target.get("domain"))
    address = address_support(target.get("address"), signals["address"])
    target_municipality = set(_tokens(target.get("municipality")))
    candidate_municipality = set(_tokens(signals["municipality"]))
    municipality_conflict = bool(
        target_municipality and candidate_municipality and not (candidate_municipality & target_municipality)
    )
    org_conflict = bool(candidate_org and target_org and candidate_org != target_org)
    org_match = bool(candidate_org and target_org and candidate_org == target_org)

    if org_conflict:
        status = MISMATCHED
        reasons = ["candidate declares a different organisation number than the target organisation"]
    elif org_match:
        status = EXACT
        reasons = ["candidate organisation number matches the target organisation number"]
    elif name_exact and domain_match:
        status = EXACT
        reasons = ["normalized legal name and registered domain both match the target"]
    elif name_exact and address["supported"]:
        status = AMBIGUOUS
        reasons = [
            "normalized legal name matches but no organisation number or registered domain confirms the exact entity",
            "supporting address evidence is never sufficient on its own to establish exact identity",
        ]
    elif name_exact or domain_match:
        status = AMBIGUOUS
        reasons = ["candidate carries partial identity evidence that cannot be resolved deterministically"]
        if municipality_conflict:
            reasons.append("candidate declares a municipality that does not match the target")
    else:
        status = UNVERIFIED
        reasons = ["candidate carries no usable organisation number, legal name or registered domain"]

    return {
        "status": status,
        "publishable": status == EXACT,
        "reasons": reasons,
        "method": METHOD,
        "signals": {
            "organisation_number_match": org_match,
            "organisation_number_conflict": org_conflict,
            "candidate_organisation_number": candidate_org,
            "name_exact": name_exact,
            "name_score": round(name_score, 4),
            "domain_match": domain_match,
            "candidate_domain": signals["domain"],
            "target_domain": str(target.get("domain") or ""),
            "address_support": address,
            "municipality_conflict": municipality_conflict,
        },
    }


def _candidate_signature(candidate: dict[str, Any]) -> tuple[str, str, str]:
    signals = _candidate_signals(candidate)
    return (
        signals["organisation_number"],
        signals["domain"],
        " ".join(sorted(set(_tokens(signals["name"])))),
    )


def resolve_candidate_identities(target: dict[str, Any], candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Triangulate a set of candidates and refuse to guess between distinct ones.

    Mirrors the existing places gate, which returns nothing on an
    ``ambiguous_exact_match``. When more than one distinct candidate identity reaches
    ``exact``, every one of them is downgraded to ``ambiguous`` so nothing is
    published rather than picking a winner.
    """
    candidates = list(candidates or [])
    decisions = [triangulate_identity(target, candidate) for candidate in candidates]
    verified_groups: dict[tuple[str, str, str], list[int]] = {}
    for index, (candidate, decision) in enumerate(zip(candidates, decisions)):
        if decision["status"] == EXACT:
            verified_groups.setdefault(_candidate_signature(candidate), []).append(index)
    if len(verified_groups) > 1:
        for indexes in verified_groups.values():
            for index in indexes:
                decisions[index] = {
                    **decisions[index],
                    "status": AMBIGUOUS,
                    "publishable": False,
                    "reasons": [
                        *decisions[index]["reasons"],
                        "several distinct candidates match the target and cannot be resolved deterministically",
                    ],
                }
    return decisions
