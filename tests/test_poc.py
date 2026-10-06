from __future__ import annotations

import json
import gzip
import csv
import sys
import os
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from norway_company_agent.evidence import evidence  # noqa: E402
from norway_company_agent.claims import (  # noqa: E402
    CLAIM_CLASSIFICATION,
    CLAIM_CONFLICT,
    CLAIM_PUBLISHED,
    METHOD,
    assemble_claims,
    build_evidence_and_claims,
    claim_id,
    diff_claims,
    evidence_id,
    normalize_claim_value,
    structured_facts,
)
from norway_company_agent.crawl_events import extract_page_event, merge_profile_events, missing_seed_error_events  # noqa: E402
from norway_company_agent.discovery import build_company_search_query, choose_search_candidate, parse_brave_web_results, score_search_candidate  # noqa: E402
from norway_company_agent.official import _reserve_history_slot, accounting_obligation_assessment, normalize_entity, normalize_financial_history, normalize_financials, normalize_roles  # noqa: E402
from norway_company_agent.operations import domain_request_summary, latency_summary, percentile  # noqa: E402
from norway_company_agent.sampling import deterministic_extension_sample, deterministic_financial_filer_sample, deterministic_website_audit_sample, financial_filer_eligible, normalize_row, stratum  # noqa: E402
from norway_company_agent.research import answer_profile, parse_screen_query, screen_profiles  # noqa: E402
from norway_company_agent.workspace import load_workspace, record_screen, save_workspace  # noqa: E402
from norway_company_agent.refresh import diff_datasets, diff_profile  # noqa: E402
from norway_company_agent.sentiment import aggregate_company_sentiment, evaluate_predictions, publishable_sentiment_item, sentiment_input_eligibility  # noqa: E402
from norway_company_agent.external_footprint import aggregate_footprint, publishable_observation, validate_observation  # noqa: E402
from norway_company_agent.external_tasks import plan_external_tasks  # noqa: E402
from norway_company_agent.external_control import development_score, run_company_control, strategy_order  # noqa: E402
from norway_company_agent.identity import apply_website_identity_gate, assess_social_identity, assess_website_identity  # noqa: E402
from norway_company_agent.identity_triangulation import (  # noqa: E402
    AMBIGUOUS,
    EXACT,
    IDENTITY_DECISIONS,
    MISMATCHED,
    UNVERIFIED,
    address_support,
    canonical_identity_from_profile,
    canonical_identity_from_task,
    name_match,
    normalized_domain,
    resolve_candidate_identities,
    same_site,
    triangulate_identity,
)
from norway_company_agent.website import _extraction_state, _priority_links, _social_links, assert_public_url, normalize_homepage, normalize_social_url, structured_social_links  # noqa: E402
from norway_company_agent.batch import evidence_terminal_state, profile_complete_for_modules, read_organisation_inputs, terminal_envelope, validate_envelopes  # noqa: E402
from norway_company_agent.snapshots import SnapshotFetcher  # noqa: E402
from norway_company_agent.connectors.base import (  # noqa: E402
    FAILED,
    NOT_AVAILABLE,
    FAILURE_BUDGET_DENIED,
    FAILURE_CONNECTOR_FAILED,
    FAILURE_INVALID_TASK,
    FAILURE_RIGHTS_NOT_APPROVED,
    FAILURE_UNKNOWN_CONNECTOR,
    FAILURE_UNSUPPORTED_ACQUISITION_MODE,
    FAILURE_UNSUPPORTED_TASK_TYPE,
    BaseConnector,
    ConnectorResult,
)
from norway_company_agent.connectors.budget import RequestBudget  # noqa: E402
from norway_company_agent.connectors.executor import STAGE_ACQUISITION_GATE, STAGE_ESTIMATE_COERCION, STAGE_EXECUTION, STAGE_REQUEST_ESTIMATION, STAGE_RIGHTS_GATE, STAGE_SUPPORT_CHECK, STAGE_TASK_VALIDATION, run_external_step, run_external_tasks  # noqa: E402
from norway_company_agent.connectors.registry import ConnectorRegistry, default_registry  # noqa: E402
from norway_company_agent.connectors.testing import MockConnector, mock_registry, mock_task  # noqa: E402
from norway_company_agent.snapshots import SnapshotFetcher  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402
from scripts.build_prototype import compact as compact_prototype, qualification_copy  # noqa: E402
from scripts.run_brave_discovery import brave_search  # noqa: E402
from scripts.run_annual_report_workforce_connector import extract_candidate, needs_ocr  # noqa: E402
from scripts.normalize_google_maps_results import candidate_score  # noqa: E402
from scripts.run_scrapy_websites import terminal_events_for_run  # noqa: E402
from scripts.run_sentiment_model import MODEL_REVISION, normalize_generated_label  # noqa: E402
from scripts.score_company_completeness import score_rows, summarize  # noqa: E402
from scripts.extract_company_site_activity import observation as site_activity_observation  # noqa: E402
from scripts.extract_company_site_news import observation as site_news_observation  # noqa: E402
from scripts.build_verified_observations import build as build_verified_observations  # noqa: E402
from scripts.run_google_news_rss_connector import exact_title_match  # noqa: E402
from scripts.run_linkedin_guest_jobs_connector import canonical_company_url, parse_detail_company_urls, parse_job_cards, parse_typeahead  # noqa: E402
from scripts.run_linkedin_guest_experiment import (  # noqa: E402
    assess_profile_identity as assess_linkedin_profile_identity,
    extract_profile as extract_linkedin_profile,
    legal_name_profile_url,
)
from scripts.run_fagfolkguiden_reviews_connector import extract_aggregate_rating, slug  # noqa: E402
from scripts.discover_linkedin_company_profiles import (  # noqa: E402
    discovery_identity as linkedin_discovery_identity,
    normalized_full_name as linkedin_normalized_full_name,
    official_site_aliases as linkedin_official_site_aliases,
    parse_exact_typeahead as parse_linkedin_exact_typeahead,
)


class EvidenceTests(unittest.TestCase):
    def test_missing_is_not_zero_and_provenance_is_required(self):
        record = evidence("financials", "not_found", "official_annual_accounts", "https://example.test/123")
        self.assertIsNone(record["value"])
        self.assertNotEqual(record["value"], 0)
        self.assertTrue(record["source_url"])
        self.assertTrue(record["retrieved_at"])

    def test_available_zero_is_preserved(self):
        record = evidence("employees", "available", "official_registry_bulk", "https://example.test", value=0)
        self.assertEqual(record["value"], 0)
        self.assertEqual(record["status"], "available")

    def test_content_hash_can_be_carried_with_evidence(self):
        record = evidence("entity", "available", "official", "https://example.test", value={}, content_sha256="a" * 64, source_row_key="999999999")
        self.assertEqual(record["content_sha256"], "a" * 64)
        self.assertEqual(record["source_class"], "official")
        self.assertEqual(record["source_row_key"], "999999999")

    def test_not_fetched_is_distinct_from_not_applicable(self):
        record = evidence("history", "not_fetched", "official", "https://example.test", note="No filing flag in snapshot")
        self.assertEqual(record["status"], "not_fetched")
        self.assertNotEqual(record["status"], "not_applicable")


class ExternalFootprintTests(unittest.TestCase):
    def observation(self, **changes):
        base = {
            "id": "obs-1",
            "organisation_number": "923609016",
            "platform": "google_places",
            "signal_type": "review",
            "source_url": "https://maps.google.com/example",
            "retrieved_at": "2026-08-20T00:00:00Z",
            "content_sha256": "a" * 64,
            "exact_entity": True,
            "identity_proof": [{"type": "address_match", "value": "Oslo"}],
            "acquisition_mode": "official_api",
            "rights_status": "approved",
            "source_class": "customer_review",
            "evidence_span": "Helpful staff",
        }
        return {**base, **changes}

    def test_publication_requires_rights_identity_hash_and_span(self):
        self.assertTrue(publishable_observation(self.observation()))
        bad = self.observation(exact_entity=False, content_sha256=None, evidence_span=None, rights_status="unknown")
        reasons = validate_observation(bad)
        self.assertIn("exact legal entity is not verified", reasons)
        self.assertIn("missing content hash", reasons)
        self.assertIn("missing evidence span", reasons)
        self.assertIn("source rights are not approved", reasons)

    def test_unofficial_scraper_output_is_experimental_not_publishable(self):
        item = self.observation(platform="linkedin", signal_type="job_posting", acquisition_mode="jobspy_experiment")
        self.assertFalse(publishable_observation(item))

    def test_linkedin_guest_jobs_require_exact_verified_company_url(self):
        raw = b'''<div class="base-search-card" data-entity-urn="urn:li:jobPosting:4456746433">
          <a class="base-card__full-link" href="https://no.linkedin.com/jobs/view/example-4456746433?x=1"></a>
          <span class="sr-only">Project manager</span>
          <h4 class="base-search-card__subtitle"><a href="https://no.linkedin.com/company/af-gruppen?trk=x">AF Gruppen</a></h4>
          <span class="job-search-card__location">Oslo</span><time datetime="2026-08-23"></time>
        </div>
        <div class="base-search-card" data-entity-urn="urn:li:jobPosting:4456746434">
          <a class="base-card__full-link" href="https://linkedin.com/jobs/view/other-4456746434"></a>
          <span class="sr-only">Wrong parent job</span>
          <h4 class="base-search-card__subtitle"><a href="https://linkedin.com/company/af-gruppen-sverige">AF Gruppen Sverige</a></h4>
        </div>'''
        jobs, candidates = parse_job_cards(raw, "https://linkedin.com/company/af-gruppen")
        self.assertEqual(candidates, 2)
        self.assertEqual([item["job_id"] for item in jobs], ["4456746433"])
        self.assertEqual(jobs[0]["company_url"], "https://linkedin.com/company/af-gruppen")

    def test_linkedin_company_urls_and_typeahead_are_normalized_without_claiming_ambiguous_ids(self):
        self.assertEqual(
            canonical_company_url("https://no.linkedin.com/company/Norsk-Fiskeeksport/about?trk=x"),
            "https://linkedin.com/company/norsk-fiskeeksport",
        )
        candidates = parse_typeahead(
            json.dumps([
                {"id": "34440", "type": "COMPANY", "displayName": "AF Gruppen"},
                {"id": "1188022", "type": "COMPANY", "displayName": "AF Gruppen Sverige"},
            ]).encode(),
            "AF GRUPPEN ASA",
        )
        self.assertTrue(candidates[0]["exact_legal_name_core"])
        self.assertFalse(candidates[1]["exact_legal_name_core"])
        self.assertEqual(
            parse_detail_company_urls(
                b'<a href="https://no.linkedin.com/company/af-gruppen?trk=job">AF Gruppen</a>'
                b'<a href="https://example.test/company/wrong">Wrong</a>'
            ),
            {"https://linkedin.com/company/af-gruppen"},
        )

    def test_linkedin_guest_profile_uses_structured_company_data_and_ignores_dormant_challenge_code(self):
        graph = {
            "@graph": [
                {
                    "@type": "DiscussionForumPosting",
                    "author": {"url": "https://no.linkedin.com/company/af-gruppen"},
                    "datePublished": "2026-08-21T06:15:05Z",
                    "text": "Exact company update",
                    "url": "https://no.linkedin.com/posts/example-activity-7496449781678927873-x",
                },
                {
                    "@type": "Organization",
                    "name": "AF Gruppen",
                    "url": "https://no.linkedin.com/company/af-gruppen",
                    "description": "Construction group",
                    "numberOfEmployees": {"value": 1303},
                },
            ]
        }
        raw = (
            '<meta name="description" content="AF Gruppen | 56 726 followers on LinkedIn">'
            f'<script type="application/ld+json">{json.dumps(graph)}</script>'
            '<script>const dormant="recaptcha/challengepage";</script>'
            '<div data-test-id="about-us__size"><dd>5,001-10,000 employees</dd></div>'
            '<article class="main-feed-activity-card" data-activity-urn="urn:li:activity:7496449781678927873">'
            '<a data-test-id="social-actions__reactions" data-num-reactions="29"></a>'
            '<a data-test-id="social-actions__comments" data-num-comments="4"></a></article>'
        ).encode()
        profile = extract_linkedin_profile(raw, "https://linkedin.com/company/af-gruppen")
        self.assertEqual(profile["followers"], 56726)
        self.assertEqual(profile["visible_employees"], 1303)
        self.assertEqual(profile["employee_size_label"], "5,001-10,000 employees")
        self.assertEqual(profile["posts"][0]["likes"], 29)
        self.assertEqual(profile["posts"][0]["comments"], 4)

    def test_linkedin_guest_profile_rejects_authwall_without_organization_data(self):
        with self.assertRaisesRegex(RuntimeError, "no structured organization"):
            extract_linkedin_profile(b'<script>recaptcha/challengepage</script>', "https://linkedin.com/company/example")

    def test_linkedin_stale_handle_fallback_is_bounded_to_registry_legal_name(self):
        self.assertEqual(legal_name_profile_url("DIPS AS"), "https://www.linkedin.com/company/dips-as")
        self.assertEqual(legal_name_profile_url("RØD & BLÅ AS"), "https://www.linkedin.com/company/rod-bla-as")

    def test_linkedin_discovery_requires_exact_typeahead_name_and_corroboration(self):
        raw = json.dumps([
            {"id": "1", "type": "COMPANY", "displayName": "DIPS AS"},
            {"id": "2", "type": "COMPANY", "displayName": "DIPS ASA"},
        ]).encode()
        self.assertEqual([item["linkedin_company_id"] for item in parse_linkedin_exact_typeahead(raw, "DIPS AS")], ["1"])
        self.assertEqual(linkedin_normalized_full_name("RØD & BLÅ AS"), "rød blå as")
        company = {
            "name": "DIPS AS",
            "municipality": "BODØ",
            "website": "https://dips.com",
            "evidence": {"website": {"status": "available", "value": {"final_url": "https://dips.com"}}},
        }
        exact = linkedin_discovery_identity(company, {"name": "DIPS AS", "website": "https://www.dips.com", "headquarters": "Bodø"}, {"legal_name_slug"})
        self.assertTrue(exact["exact_entity"])
        weak = linkedin_discovery_identity(company, {"name": "DIPS AS", "website": "https://unrelated.test", "headquarters": "Oslo"}, {"legal_name_slug"})
        self.assertFalse(weak["exact_entity"])

    def test_linkedin_fuzzy_discovery_uses_verified_site_alias_and_reverse_domain(self):
        company = {
            "name": "JARRE AS",
            "municipality": "INDRE ØSTFOLD",
            "website": "https://jarre.co",
            "evidence": {
                "website": {"status": "available", "value": {"final_url": "https://jarre.co", "title": "Jarre&Co"}},
                "roles": {"value": {"roles": [{"name": "Christian Jarre", "role_code": "DAGL"}]}},
            },
        }
        self.assertEqual(linkedin_official_site_aliases(company), ["Jarre&Co"])
        exact = linkedin_discovery_identity(
            company,
            {"name": "Jarre & Co", "website": "https://www.jarre.co", "headquarters": "Askim", "description": ""},
            {"official_site_alias:Jarre&Co"},
        )
        self.assertTrue(exact["exact_entity"])

    def test_linkedin_profile_identity_accepts_redirect_alias_only_with_name_or_reverse_domain_proof(self):
        profile = {
            "name": "ZAPTEC ASA",
            "website": "https://zaptec.com",
            "evidence": {"website": {"source_url": "https://www.zaptec.com/", "value": {"final_url": "https://www.zaptec.com/"}}},
        }
        accepted = assess_linkedin_profile_identity(
            profile,
            "https://linkedin.com/company/gozaptec",
            {"name": "Zaptec", "page_url": "https://linkedin.com/company/zaptec", "website": "https://www.zaptec.com"},
        )
        self.assertTrue(accepted["publishable_candidate"])
        rejected = assess_linkedin_profile_identity(
            profile,
            "https://linkedin.com/company/gozaptec",
            {"name": "Unrelated Parent", "page_url": "https://linkedin.com/company/unrelated", "website": "https://parent.test"},
        )
        self.assertFalse(rejected["publishable_candidate"])

    def test_google_play_observation_is_supported_but_unofficial_output_stays_experimental(self):
        item = self.observation(
            platform="google_play",
            signal_type="review_summary",
            acquisition_mode="unofficial_api_experiment",
            rights_status="review_required",
        )
        reasons = validate_observation(item)
        self.assertNotIn("unsupported platform", reasons)
        self.assertFalse(publishable_observation(item))

    def test_company_directory_is_not_a_website_discovery_candidate(self):
        profile = {"name": "OBLOMOV AS", "organisation_number": "991167315", "municipality": "SOLA"}
        result = {"url": "https://www.northdata.com/Oblomov-AS/BR-991167315", "title": "Oblomov AS", "snippet": "991167315", "rank": 1}
        assessment = score_search_candidate(profile, result)
        self.assertFalse(assessment["publishable_candidate"])
        self.assertEqual(assessment["status"], "rejected")

    def test_unknown_company_directory_with_org_number_is_not_a_candidate(self):
        profile = {"name": "AKSLA AS", "organisation_number": "923304290", "municipality": "ÅLESUND"}
        result = {"url": "https://vexter.no/selskap/aksla-as/923304290", "title": "AKSLA AS", "snippet": "923304290", "rank": 1}
        self.assertFalse(score_search_candidate(profile, result)["publishable_candidate"])

    def test_annual_workforce_parser_does_not_treat_norwegian_o_as_zero(self):
        heading = "Note 2 - Lonnskostnader, antall ansatte og lan til ansatte"
        self.assertEqual(extract_candidate(heading), (None, None, "no_employee_phrase", None))
        count, span, status, measure = extract_candidate("Det er to ansatte i sameiet.")
        self.assertEqual((count, status, measure), (2, "accepted", "employees"))
        self.assertEqual(span, "Det er to ansatte i sameiet.")
        self.assertEqual(extract_candidate("Selskapet har 1 2025 sysselsatt 2 arsverk.")[0], 2)
        self.assertEqual(extract_candidate("Antall arsverk syssetsatt i regnskapsaret: 3")[0], 3)
        self.assertEqual(extract_candidate("Stiftelsen har ingen ansatte og ingen arsverk.")[0], 0)
        self.assertEqual(extract_candidate("Selskapet hadde ingen ansatte i 2025.")[0], 0)
        self.assertEqual(extract_candidate("Gjennomsnittlig antall ansatte i regnskapsaret: 0")[0], 0)
        self.assertEqual(extract_candidate("Note Antall Aarsverk i regnskapsaret 0.00")[0], 0)
        self.assertEqual(extract_candidate("Tal pa Aarsverk i rekneskapsaret 1.50")[0], 1.5)
        self.assertTrue(needs_ocr("Digital cover text without the employee note"))
        self.assertFalse(needs_ocr("Selskapet har 2 ansatte. " + "Digital report text. " * 8))

    def test_aggregate_keeps_source_metrics_separate_and_abstains_on_thin_sentiment(self):
        items = [
            self.observation(id="a", sentiment_label="positive", sentiment_model_version="m1"),
            self.observation(id="b", platform="youtube", signal_type="profile_metrics", source_url="https://youtube.com/@example", evidence_span=None),
        ]
        result = aggregate_footprint(items, as_of="2026-08-22T00:00:00Z")
        self.assertEqual(result["accepted_observations"], 2)
        self.assertEqual(result["sentiment"]["status"], "abstain")
        self.assertNotIn("popularity_score", result)

    def test_customer_review_sentiment_accepts_ten_independent_reviewers_on_one_platform(self):
        items = [
            self.observation(
                id=f"review-{index}",
                sentiment_label="positive",
                sentiment_model_version="explicit_star_rating_v1",
                reviewer_id=f"reviewer-{index}",
            )
            for index in range(10)
        ]
        result = aggregate_footprint(items, as_of="2026-08-22T00:00:00Z")
        self.assertEqual(result["sentiment"]["status"], "available")
        self.assertEqual(result["sentiment"]["independent_reviewers"], 10)

    def test_google_maps_identity_gate_rejects_neighbor_and_accepts_exact_address(self):
        profile = {
            "organisation_number": "938702675",
            "name": "AF GRUPPEN ASA",
            "evidence": {
                "registry": {"value": {
                    "forretningsadresse.adresse": "Standardveien 1",
                    "forretningsadresse.postnummer": "0581",
                    "telefon": "22 89 11 00",
                }},
                "website": {"value": {
                    "final_url": "https://afgruppen.no/",
                    "identity_assessment": {"publishable": True},
                }},
            },
        }
        exact = candidate_score(profile, {
            "title": "AF Gruppen", "address": "Standardveien 1, 0581 Oslo, Norge",
            "phone": "+47 22 89 11 00", "web_site": "https://afgruppen.no/", "review_count": 21,
        })
        neighbor = candidate_score(profile, {
            "title": "AF Eiendom", "address": "Standardveien 1, 0581 Oslo, Norge",
            "phone": "+47 22 89 11 00", "web_site": "https://afgruppen.no/eiendom/", "review_count": 0,
        })
        self.assertTrue(exact["accepted"])
        self.assertFalse(neighbor["accepted"])

    def test_google_maps_exact_name_and_postcode_city_can_resolve_operating_address(self):
        profile = {
            "organisation_number": "999999999",
            "name": "EXAMPLE INDUSTRI AS",
            "evidence": {"registry": {"value": {
                "forretningsadresse.adresse": "c/o Accountant Other Street 1",
                "forretningsadresse.postnummer": "4021",
                "forretningsadresse.poststed": "STAVANGER",
            }}},
        }
        result = candidate_score(profile, {
            "title": "Example Industri AS", "address": "Factory Road 7, 4021 Stavanger, Norway",
            "phone": "", "web_site": "", "review_count": 4,
        })
        self.assertTrue(result["accepted"])
        self.assertTrue(result["postcode_city_match"])

    def test_google_maps_trade_name_requires_exact_address_phone_and_no_partial_name_collision(self):
        profile = {
            "name": "OSLOFJORDEN EIENDOMSMEGLING AS",
            "evidence": {"registry": {"value": {
                "forretningsadresse.adresse": "Stranden 81", "forretningsadresse.postnummer": "0250",
                "forretningsadresse.poststed": "Oslo", "telefon": "22620000",
            }}},
        }
        candidate = {"title": "PrivatMegleren Premium", "address": "Stranden 81, 0250 Oslo", "phone": "+47 22 62 00 00"}
        result = candidate_score(profile, candidate)
        self.assertFalse(result["trade_name_match"])
        self.assertFalse(result["accepted"])
        profile["organisation_number"] = "932083108"
        result = candidate_score(profile, candidate)
        self.assertTrue(result["trade_name_match"])
        self.assertTrue(result["accepted"])
        candidate["phone"] = "+47 99 99 99 99"
        self.assertFalse(candidate_score(profile, candidate)["accepted"])

    def test_experimental_maps_signals_raise_only_experimental_places_score(self):
        profile = {
            "organisation_number": "938702675",
            "name": "AF GRUPPEN ASA",
            "evidence": {"website": {"value": {"identity_assessment": {"publishable": False}}}},
        }
        common = {
            "organisation_number": "938702675",
            "platform": "google_places",
            "source_url": "https://www.google.com/maps/place/example",
            "retrieved_at": "2026-08-22T00:00:00Z",
            "content_sha256": "a" * 64,
            "exact_entity": True,
            "identity_proof": [{"type": "registry_address_match", "value": True}],
            "acquisition_mode": "unofficial_api_experiment",
            "rights_status": "review_required",
            "source_class": "public_business_listing",
            "evidence_span": "AF Gruppen; Standardveien 1; rating=2.5; reviews=21",
        }
        observations = [
            {**common, "id": "place", "signal_type": "place_summary", "strategy": "places_identity_resolution"},
            {**common, "id": "summary", "signal_type": "review_summary", "strategy": "places_rating_reviews"},
        ]
        score = development_score(profile, observations)
        self.assertEqual(score["score"], 0.0)
        self.assertEqual(score["experimental_potential_score"], 35.0)

    def test_aggregate_maps_rating_is_experimental_sentiment_and_buzz(self):
        profile = {
            "organisation_number": "938702675",
            "name": "AF GRUPPEN ASA",
            "evidence": {"website": {"value": {"identity_assessment": {"publishable": False}}}},
        }
        common = {
            "organisation_number": "938702675",
            "platform": "google_places",
            "source_url": "https://www.google.com/maps/place/example",
            "retrieved_at": "2026-08-22T00:00:00Z",
            "content_sha256": "a" * 64,
            "exact_entity": True,
            "identity_proof": [{"type": "registry_address_match", "value": True}],
            "acquisition_mode": "unofficial_api_experiment",
            "rights_status": "review_required",
            "evidence_span": "AF Gruppen; rating=4.4; reviews=21",
            "metrics": {"rating": 4.4, "rating_scale": 5, "review_count": 21},
        }
        observations = [
            {**common, "id": "summary", "signal_type": "review_summary", "strategy": "places_rating_reviews"},
            {**common, "id": "buzz", "signal_type": "buzz_metrics", "strategy": "buzz_peer_normalization"},
        ]
        score = development_score(profile, observations)
        self.assertEqual(score["score"], 0.0)
        self.assertEqual(score["experimental_sentiment_status"], "available")
        self.assertEqual(score["experimental_potential_score"], 35.0)

    def test_task_planner_uses_verified_handles_and_adds_core_connectors(self):
        profile = {
            "organisation_number": "923609016",
            "name": "Example AS",
            "evidence": {
                "website": {"value": {"social_links": [{"platform": "youtube", "url": "https://youtube.com/@example"}]}},
            },
        }
        tasks = plan_external_tasks(profile)
        connectors = {item["connector"] for item in tasks}
        self.assertIn("google_places_api", connectors)
        self.assertIn("jobs_provider", connectors)
        self.assertIn("youtube_connector", connectors)
        self.assertNotIn("permitted_search_api", connectors)

    def test_controller_recomputes_sentiment_and_records_marginal_gain(self):
        profile = {
            "organisation_number": "923609016",
            "name": "Example AS",
            "evidence": {"website": {"value": {"identity_assessment": {"publishable": True}}}},
        }
        handle = self.observation(signal_type="profile_handle", source_class="company_social", evidence_span=None, strategy="verified_handle_extraction")
        score = development_score(profile, [handle])
        self.assertGreater(score["score"], 0)
        self.assertEqual(score["sentiment_status"], "abstain")
        result = run_company_control(profile, [handle], minimum_iterations=10, maximum_iterations=15)
        self.assertGreaterEqual(result["iterations_run"], 10)
        self.assertTrue(any(item["score_delta"] > 0 for item in result["iterations"]))
        self.assertTrue(all("sentiment_status" in item for item in result["iterations"]))

    def test_controller_final_score_is_not_path_dependent_after_target_is_reached(self):
        profile = {
            "organisation_number": "923609016",
            "name": "Example AS",
            "evidence": {"website": {"value": {"identity_assessment": {"publishable": True}}}},
        }
        observations = [
            self.observation(signal_type="profile_handle", source_class="company_social", evidence_span=None, strategy="verified_handle_extraction"),
            self.observation(id="metric", platform="youtube", signal_type="profile_metrics", source_url="https://youtube.com/@example", evidence_span=None, strategy="social_profile_metrics"),
        ]
        result = run_company_control(profile, observations, target=20, minimum_iterations=1)
        self.assertEqual(result["iterations_run"], len(strategy_order([])))
        self.assertEqual(result["final"], development_score(profile, observations))

    def test_exact_wikidata_org_profile_can_supply_external_identity(self):
        profile = {
            "organisation_number": "923609016",
            "name": "Example AS",
            "evidence": {"website": {"value": {"identity_assessment": {"publishable": False}}}},
        }
        wikidata = self.observation(
            platform="wikidata",
            signal_type="company_profile",
            source_url="https://www.wikidata.org/wiki/Q123",
            evidence_span="Q123: P2333=923609016",
            source_class="open_knowledge_graph",
            strategy="company_site_identity",
        )
        score = development_score(profile, [wikidata])
        self.assertEqual(score["components"]["exact_external_identity"], 20.0)
        self.assertTrue(publishable_observation(wikidata))

    def test_controller_replicates_prior_winning_strategy_first(self):
        prior = [
            {"strategy": "youtube_channel_feed", "learning_gain": 8.0},
            {"strategy": "verified_handle_extraction", "learning_gain": 2.0},
        ]
        self.assertEqual(strategy_order(prior)[0], "youtube_channel_feed")
        profile = {"organisation_number": "923609016", "name": "Example AS", "evidence": {"website": {"value": {"identity_assessment": {"publishable": True}}}}}
        result = run_company_control(profile, [], prior_iterations=prior, minimum_iterations=1, maximum_iterations=2)
        self.assertEqual(result["iterations"][0]["strategy"], "youtube_channel_feed")
        self.assertEqual(result["iterations"][0]["controller_action"], "replicate")


class CompletenessScoreTests(unittest.TestCase):
    def test_all_source_weights_sum_to_one_hundred(self):
        from scripts.score_company_completeness import ENRICHMENT_WEIGHTS, FOUNDATION_WEIGHTS

        self.assertEqual(sum(FOUNDATION_WEIGHTS.values()) + sum(ENRICHMENT_WEIGHTS.values()), 100.0)

    def test_all_source_score_combines_foundation_and_external_without_imputing_missing(self):
        profile = {
            "organisation_number": "923609016",
            "evidence": {
                "registry_live": {"status": "available", "value": {"organisation_number": "923609016"}},
                "financials": {"status": "available"},
                "roles": {"status": "available"},
                "locations": {"status": "available"},
                "website": {"status": "not_found"},
            },
        }
        components = {
            "exact_external_identity": 20,
            "verified_handles": 15,
            "profile_metrics": 10,
            "places_identity": 5,
            "places_reviews": 10,
            "workforce_jobs": 10,
            "public_buzz": 10,
            "independent_sentiment": 0,
            "freshness_evidence": 5,
        }
        result = {"organisation_number": "923609016", "company_name": "Example AS", "final": {"components": components, "experimental_components": components}}
        scored = score_rows([profile], [result])
        self.assertEqual(scored[0]["foundation_score"], 30.0)
        self.assertEqual(scored[0]["strict_enrichment"]["independent_sentiment"], 0.0)
        self.assertEqual(scored[0]["strict_completeness_score"], 88.0)
        self.assertEqual(summarize(scored)["companies"], 1)

    def test_site_activity_requires_exact_identity_and_preserves_snapshot_provenance(self):
        profile = {
            "organisation_number": "923609016",
            "evidence": {"website": {
                "status": "available",
                "source_url": "https://example.test/",
                "retrieved_at": "2026-08-23T00:00:00Z",
                "value": {
                    "final_url": "https://example.test/",
                    "content_sha256": "a" * 64,
                    "identity_assessment": {"publishable": True, "status": "exact", "score": 1.0},
                    "pages": [{"url": "https://example.test/"}],
                },
            }},
        }
        item = site_activity_observation(profile)
        self.assertIsNotNone(item)
        self.assertEqual(item["strategy"], "company_site_activity")
        self.assertTrue(publishable_observation(item))
        profile["evidence"]["website"]["value"]["identity_assessment"]["publishable"] = False
        self.assertIsNone(site_activity_observation(profile))

    def test_news_title_gate_requires_the_full_legal_name_core(self):
        self.assertTrue(exact_title_match("NORDIC DOOR AS", "Nordic Door AS åpner ny fabrikk - Lokalavisa"))
        self.assertFalse(exact_title_match("NORDIC DOOR AS", "Nordic investors prefer another door - Example"))
        self.assertTrue(exact_title_match("SOLVANG ASA", "Sterkt årsresultat fra Solvang ASA i 2024 - Skipsrevyen"))
        self.assertFalse(exact_title_match("VIND HOLDING AS", "Inntektene til Aneo Roan Vind Holding AS stupte - mn24.no"))
        self.assertFalse(exact_title_match("CONSTO AS", "Drastisk fall hos Consto Bergen AS - BT"))

    def test_site_news_requires_exact_identity_and_a_captured_news_path(self):
        profile = {
            "organisation_number": "923609016",
            "evidence": {"website": {
                "status": "available", "retrieved_at": "2026-08-23T00:00:00Z",
                "value": {
                    "identity_assessment": {"publishable": True, "score": 1.0},
                    "pages": [{"url": "https://example.test/aktuelt/new-contract", "title": "New contract", "content_sha256": "a" * 64}],
                },
            }},
        }
        item = site_news_observation(profile)
        self.assertEqual(item["signal_type"], "public_post")
        self.assertTrue(publishable_observation(item))
        profile["evidence"]["website"]["value"]["pages"][0]["url"] = "https://example.test/contact"
        self.assertIsNone(site_news_observation(profile))

    def test_verified_observations_require_known_org_and_snapshot_hash(self):
        profiles = [{"organisation_number": "923609016", "name": "Example AS"}]
        seed = {"organisation_number": "923609016", "platform": "news", "signal_type": "public_mention", "source_url": "https://example.test/news", "content_sha256": "a" * 64, "evidence_span": "Example AS", "proof": "Exact legal name"}
        self.assertTrue(build_verified_observations([seed], profiles)[0]["exact_entity"])
        seed["content_sha256"] = "bad"
        with self.assertRaises(ValueError):
            build_verified_observations([seed], profiles)

    def test_directory_identity_is_experimental_and_never_becomes_strict(self):
        item = {
            "id": "directory-1", "organisation_number": "923609016", "platform": "company_directory",
            "signal_type": "company_profile", "source_url": "https://example.test/923609016",
            "retrieved_at": "2026-08-23T00:00:00Z", "content_sha256": "a" * 64,
            "exact_entity": True, "identity_proof": [{"type": "organisation_number", "value": "923609016"}],
            "acquisition_mode": "rights_review_experiment", "rights_status": "review_required",
            "source_class": "public_company_directory", "strategy": "company_directory_identity",
        }
        profile = {"organisation_number": "923609016", "name": "Example AS", "evidence": {}}
        score = development_score(profile, [item])
        self.assertEqual(score["components"]["exact_external_identity"], 0.0)
        self.assertEqual(score["experimental_components"]["exact_external_identity"], 20.0)
        self.assertFalse(publishable_observation(item))

    def test_fagfolk_rating_parser_uses_jsonld_and_slug_is_stable(self):
        raw = b'<script type="application/ld+json">{"aggregateRating":{"ratingValue":4.4,"ratingCount":25}}</script>'
        self.assertEqual(extract_aggregate_rating(raw)[:2], (4.4, 25))
        self.assertEqual(slug("NORDIC DØR AS"), "nordic-dor-as")


class SamplingTests(unittest.TestCase):
    def test_financial_filer_sample_requires_current_active_rows_and_preserves_overlap(self):
        fields = ["organisasjonsnummer", "navn", "organisasjonsform.kode", "sisteInnsendteAarsregnskap", "konkurs", "underAvvikling"]
        rows = [
            {"organisasjonsnummer": str(200000000 + index), "navn": f"Company {index}", "organisasjonsform.kode": "AS", "sisteInnsendteAarsregnskap": "2025", "konkurs": "false", "underAvvikling": "false"}
            for index in range(12)
        ] + [
            {"organisasjonsnummer": "300000001", "navn": "Stale AS", "organisasjonsform.kode": "AS", "sisteInnsendteAarsregnskap": "2024", "konkurs": "false", "underAvvikling": "false"},
            {"organisasjonsnummer": "300000002", "navn": "Bankrupt AS", "organisasjonsform.kode": "AS", "sisteInnsendteAarsregnskap": "2025", "konkurs": "true", "underAvvikling": "false"},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.csv.gz"
            with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields, delimiter=";")
                writer.writeheader()
                writer.writerows(rows)
            selected, metadata = deterministic_financial_filer_sample(path, 5, latest_year="2025", preserved_organisation_numbers={"200000003"}, seed=7)
            repeated, _ = deterministic_financial_filer_sample(path, 5, latest_year="2025", preserved_organisation_numbers={"200000003"}, seed=7)
        self.assertEqual([row["organisation_number"] for row in selected], [row["organisation_number"] for row in repeated])
        self.assertIn("200000003", {row["organisation_number"] for row in selected})
        self.assertNotIn("300000001", {row["organisation_number"] for row in selected})
        self.assertNotIn("300000002", {row["organisation_number"] for row in selected})
        self.assertEqual(metadata["eligible_rows"], 12)
        self.assertEqual(metadata["preserved_eligible_selected"], 1)
        self.assertTrue(all(financial_filer_eligible(row, "2025") for row in selected))

    def test_extension_sample_is_deterministic_and_excludes_initial(self):
        fields = ["organisasjonsnummer", "navn", "hjemmeside", "organisasjonsform.kode"]
        rows = [
            {"organisasjonsnummer": str(100000000 + index), "navn": f"Company {index}", "hjemmeside": "", "organisasjonsform.kode": "AS"}
            for index in range(20)
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.csv.gz"
            with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields, delimiter=";")
                writer.writeheader()
                writer.writerows(rows)
            first, metadata = deterministic_extension_sample(path, 5, {"100000000", "100000001"}, seed=3)
            second, _ = deterministic_extension_sample(path, 5, {"100000000", "100000001"}, seed=3)
        self.assertEqual([item["organisation_number"] for item in first], [item["organisation_number"] for item in second])
        self.assertEqual(len(first), 5)
        self.assertEqual(metadata["overlap_with_excluded"], 0)

    def test_strata_distinguish_adverse_and_web_coverage(self):
        base = {"legal_form": "AS", "employees": 12, "bankrupt": False, "liquidating": False, "website": "example.no"}
        self.assertEqual(stratum(base), "AS|5-19|active|web")
        self.assertEqual(stratum({**base, "bankrupt": True, "website": ""}), "AS|5-19|adverse|no-web")

    def test_normalize_does_not_invent_employee_count(self):
        row = normalize_row({"organisasjonsnummer": "923609016", "navn": "Example AS", "antallAnsatte": ""})
        self.assertIsNone(row["employees"])
        self.assertEqual(row["latest_submitted_accounts"], "")

    def test_fresh_website_audit_sample_excludes_poc_and_deduplicates_hosts(self):
        fields = ["organisasjonsnummer", "navn", "hjemmeside", "organisasjonsform.kode"]
        rows = [
            {"organisasjonsnummer": "111111111", "navn": "Excluded AS", "hjemmeside": "excluded.no", "organisasjonsform.kode": "AS"},
            {"organisasjonsnummer": "222222222", "navn": "A AS", "hjemmeside": "https://www.shared.no/a", "organisasjonsform.kode": "AS"},
            {"organisasjonsnummer": "333333333", "navn": "B AS", "hjemmeside": "shared.no/b", "organisasjonsform.kode": "AS"},
            {"organisasjonsnummer": "444444444", "navn": "C AS", "hjemmeside": "unique.no", "organisasjonsform.kode": "AS"},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.csv.gz"
            with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields, delimiter=";")
                writer.writeheader()
                writer.writerows(rows)
            selected, metadata = deterministic_website_audit_sample(path, 1, {"111111111"}, {"shared.no"}, seed=9)
        self.assertEqual(len(selected), 1)
        self.assertNotIn("111111111", {row["organisation_number"] for row in selected})
        self.assertEqual(selected[0]["organisation_number"], "444444444")
        self.assertEqual(metadata["unique_hosts_selected"], 1)
        self.assertEqual(metadata["excluded_website_hosts"], 1)


class OperationsTests(unittest.TestCase):
    def test_history_rate_limiter_spaces_request_starts_not_responses(self):
        import norway_company_agent.official as official

        old = official._history_last_request
        now = [10.0]
        sleeps = []

        def clock():
            return now[0]

        def sleeper(delay):
            sleeps.append(delay)
            now[0] += delay

        try:
            official._history_last_request = 9.0
            _reserve_history_slot(clock, sleeper)
            self.assertAlmostEqual(sleeps[0], 1.1)
            self.assertAlmostEqual(official._history_last_request, 11.1)
            now[0] = 13.3
            _reserve_history_slot(clock, sleeper)
            self.assertEqual(len(sleeps), 1)
            self.assertAlmostEqual(official._history_last_request, 13.3)
        finally:
            official._history_last_request = old

    def test_batch_input_preserves_split_annotations(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "orgs.jsonl"
            path.write_text(json.dumps({"organisation_number": "923609016", "evaluation_split": "held_out", "sample_slice": "stress", "ignored": "x"}) + "\n", encoding="utf-8")
            self.assertEqual(read_organisation_inputs(path), [{"organisation_number": "923609016", "evaluation_split": "held_out", "sample_slice": "stress"}])

    def test_batch_contract_emits_exact_terminal_envelopes(self):
        profile = {
            "organisation_number": "923609016",
            "evidence": {
                "registry": evidence("registry", "available", "official", "https://example.test", content_sha256="a" * 64),
                "website": evidence("website", "blocked", "company_site", "https://example.test", note="robots.txt denied"),
            },
        }
        envelope = terminal_envelope(profile, run_id="day-1", modules=["registry", "website"], started_at="2026-01-01T00:00:00Z", completed_at="2026-01-01T00:01:00Z")
        self.assertEqual(envelope["modules"]["registry"]["state"], "complete")
        self.assertEqual(envelope["modules"]["website"]["state"], "blocked_robots")
        self.assertTrue(validate_envelopes([envelope], 1)["passed"])
        self.assertFalse(validate_envelopes([envelope], 2)["passed"])

    def test_unknown_evidence_state_is_submission_error(self):
        self.assertEqual(evidence_terminal_state({"status": "not_fetched"}), "submission_error")

    def test_batch_resume_only_skips_profiles_with_all_terminal_modules(self):
        complete = {"evidence": {"registry": {"status": "available"}, "website": {"status": "not_found"}}}
        partial = {"evidence": {"registry": {"status": "available"}, "website": {"status": "not_fetched"}}}
        self.assertTrue(profile_complete_for_modules(complete, ["registry", "website"]))
        self.assertFalse(profile_complete_for_modules(partial, ["registry", "website"]))

    def test_nearest_rank_percentiles_are_deterministic(self):
        self.assertEqual(percentile([1, 2, 3, 4, 100], 0.5), 3)
        self.assertEqual(percentile([1, 2, 3, 4, 100], 0.95), 100)
        self.assertEqual(latency_summary([1, 2, 3]), {"n": 3, "p50_ms": 2.0, "p95_ms": 3.0, "max_ms": 3.0})

    def test_domain_fairness_summary_preserves_tail(self):
        result = domain_request_summary(__import__("collections").Counter({"a.no": 1, "b.no": 2, "c.no": 9}))
        self.assertEqual(result["domains"], 3)
        self.assertEqual(result["p50_requests"], 2.0)
        self.assertEqual(result["max_requests"], 9)


class WebsiteTests(unittest.TestCase):
    def test_interrupted_run_does_not_synthesize_terminal_failures(self):
        profiles = [{"organisation_number": "1", "website": "pending.no"}]
        self.assertEqual(terminal_events_for_run(profiles, [], False), [])
        self.assertEqual(len(terminal_events_for_run(profiles, [], True)), 1)

    def test_missing_seed_gets_explicit_terminal_event(self):
        profiles = [
            {"organisation_number": "1", "website": "example.no"},
            {"organisation_number": "2", "website": "blocked.no"},
            {"organisation_number": "3", "website": ""},
        ]
        existing = [{"organisation_number": "1", "status": "available"}]
        missing = missing_seed_error_events(profiles, existing)
        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0]["organisation_number"], "2")
        self.assertEqual(missing[0]["status"], "source_error")
        self.assertIn("robots.txt", missing[0]["error"])

    def test_crawl_event_extraction_and_merge_preserve_page_hashes(self):
        homepage = extract_page_event(
            organisation_number="923609016",
            requested_url="https://example.no/",
            final_url="https://example.no/",
            status_code=200,
            content_type="text/html; charset=utf-8",
            body=b'<html><head><title>Example AS</title><meta name="description" content="Company"></head><body><p>Example AS provides enough substantive company information for extraction and identity review.</p><a href="https://linkedin.com/company/example">LinkedIn</a></body></html>',
            page_kind="homepage",
            retrieved_at="2026-08-22T00:00:00Z",
        )
        secondary = extract_page_event(
            organisation_number="923609016",
            requested_url="https://example.no/contact",
            final_url="https://example.no/contact",
            status_code=200,
            content_type="text/html",
            body=b"<html><title>Contact</title><body>Contact Example AS in Oslo.</body></html>",
            page_kind="priority",
            retrieved_at="2026-08-22T00:00:01Z",
        )
        record = merge_profile_events({"website": "https://example.no"}, [homepage, secondary])
        self.assertEqual(record["status"], "available")
        self.assertEqual(len(record["value"]["pages"]), 2)
        self.assertEqual(record["content_sha256"], homepage["content_sha256"])
        self.assertEqual(record["value"]["scheduler"], "scrapy_resumable_v1")

    def test_footer_identity_is_preserved_for_exact_company_gate(self):
        event = extract_page_event(
            organisation_number="985628572",
            requested_url="https://netsolution.no/",
            final_url="https://netsolution.no/",
            status_code=200,
            content_type="text/html",
            body=(
                b'<html><head><title>IT services</title></head><body><main>Useful services for customers.</main>'
                b'<footer>Netsolution Viken AS, Kobbervikdalen 75 A, 3036 Drammen</footer></body></html>'
            ),
            page_kind="homepage",
            retrieved_at="2026-08-23T00:00:00Z",
        )
        website = merge_profile_events({"website": "https://netsolution.no/"}, [event])
        profile = {
            "organisation_number": "985628572",
            "name": "NETSOLUTION VIKEN AS",
            "evidence": {"website": website},
        }
        self.assertIn("Netsolution Viken AS", website["value"]["identity_text_excerpt"])
        self.assertTrue(assess_website_identity(profile)["publishable"])

    def test_normalizes_registry_hostname(self):
        self.assertEqual(normalize_homepage("example.no"), "https://example.no/")
        self.assertEqual(normalize_homepage("http://example.no"), "http://example.no/")

    def test_only_extracts_declared_social_links(self):
        soup = BeautifulSoup('<a href="https://www.linkedin.com/company/example/">LinkedIn</a><a href="/about">About</a>', "html.parser")
        self.assertEqual(_social_links("https://example.no", soup), [{"platform": "linkedin", "url": "https://linkedin.com/company/example"}])

    def test_extracts_embedded_company_social_profiles(self):
        soup = BeautifulSoup(
            '<div class="fb-page" data-href="https://www.facebook.com/ExampleCompany"></div>'
            '<iframe src="https://www.facebook.com/plugins/page.php?href=https%3A%2F%2Fwww.facebook.com%2FSecondCompany"></iframe>',
            "html.parser",
        )
        self.assertEqual(
            _social_links("https://example.no/", soup),
            [
                {"platform": "facebook", "url": "https://facebook.com/ExampleCompany"},
                {"platform": "facebook", "url": "https://facebook.com/SecondCompany"},
            ],
        )

    def test_extracts_schema_same_as_company_social_profiles(self):
        value = [{
            "@type": "Organization",
            "sameAs": [
                "https://www.facebook.com/ExampleCompany/",
                "https://instagram.com/examplecompany",
                "https://linkedin.com/in/example-person",
            ],
        }]
        self.assertEqual(
            structured_social_links(value),
            [
                {"platform": "facebook", "url": "https://facebook.com/ExampleCompany"},
                {"platform": "instagram", "url": "https://instagram.com/examplecompany"},
            ],
        )

    def test_social_profiles_reject_share_event_group_and_policy_links(self):
        rejected = (
            "https://facebook.com/sharer.php?u=x", "https://facebook.com/events/123",
            "https://facebook.com/groups/123", "https://facebook.com/policy.php",
            "https://facebook.com/privacy/explanation",
            "https://linkedin.com/shareArticle?url=x", "https://instagram.com/p/abc",
        )
        self.assertTrue(all(normalize_social_url(url) is None for url in rejected))

    def test_social_profiles_canonicalize_www_variants(self):
        self.assertEqual(normalize_social_url("https://www.facebook.com/Example/"), {"platform": "facebook", "url": "https://facebook.com/Example"})
        self.assertEqual(normalize_social_url("https://linkedin.com/company/example/admin/feed/posts"), {"platform": "linkedin", "url": "https://linkedin.com/company/example"})
        self.assertEqual(normalize_social_url("https://youtube.com/channel/abc/featured"), {"platform": "youtube", "url": "https://youtube.com/channel/abc"})
        self.assertIsNone(normalize_social_url("https://facebook.com/profile.php"))
        self.assertIsNone(normalize_social_url("https://[object Object]"))

    def test_priority_pages_stay_on_exact_site(self):
        soup = BeautifulSoup('<a href="/kontakt">Contact</a><a href="https://other.no/about">About</a><a href="/products">Products</a>', "html.parser")
        self.assertEqual(_priority_links("https://example.no/", soup), ["https://example.no/kontakt"])

    def test_js_shell_is_only_a_fallback_candidate(self):
        shell = BeautifulSoup('<html><script src="a.js"></script><script src="b.js"></script></html>', "html.parser")
        self.assertEqual(_extraction_state("", shell), "js_fallback_candidate")
        self.assertEqual(_extraction_state("A" * 100, shell), "static_complete")

    def test_blocks_local_network_targets(self):
        for url in ("http://127.0.0.1/admin", "http://localhost/", "http://169.254.169.254/latest/meta-data"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                assert_public_url(url)


class DiscoveryTests(unittest.TestCase):
    def test_brave_request_keeps_key_out_of_url_and_parses_in_memory(self):
        captured = {}

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return json.dumps({"web": {"results": [{
                    "url": "https://example.no", "title": "Example AS", "description": "Example in Oslo",
                }]}}).encode()

        def fake_open(request, timeout):
            captured["url"] = request.full_url
            captured["key"] = request.get_header("X-subscription-token")
            captured["timeout"] = timeout
            return Response()

        with patch("scripts.run_brave_discovery.urllib.request.urlopen", fake_open):
            results, operation = brave_search({
                "name": "Example AS", "organisation_number": "999999999", "municipality": "OSLO",
            }, "secret-test-key", timeout=3.0, count=5)
        self.assertEqual(len(results), 1)
        self.assertEqual(operation["status"], 200)
        self.assertNotIn("secret-test-key", captured["url"])
        self.assertEqual(captured["key"], "secret-test-key")
        self.assertEqual(captured["timeout"], 3.0)

    def test_company_query_contains_exact_name_org_and_location(self):
        query = build_company_search_query({
            "name": "Norsk Fiskeeksport AS", "organisation_number": "923 609 016", "municipality": "NOTODDEN",
        })
        self.assertEqual(query, '"Norsk Fiskeeksport AS" 923609016 NOTODDEN')

    def test_brave_parser_is_provider_neutral_candidate_input(self):
        results = parse_brave_web_results({"web": {"results": [
            {"url": "https://example.no", "title": "Example AS", "description": "Example in Oslo"},
            {"title": "Missing URL"},
        ]}}, query='"Example AS" 999999999')
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["provider"], "brave_search_api")
        self.assertEqual(results[0]["rank"], 1)

    def test_directory_and_social_results_are_not_company_site_candidates(self):
        profile = {"organisation_number": "923609016", "name": "Example Norge AS", "municipality": "OSLO"}
        for url in ("https://proff.no/selskap/example", "https://linkedin.com/company/example"):
            with self.subTest(url=url):
                self.assertEqual(score_search_candidate(profile, {"url": url, "title": "Example Norge AS"})["status"], "rejected")

    def test_exact_name_in_title_and_host_is_only_a_crawl_candidate(self):
        profile = {"organisation_number": "923609016", "name": "Norsk Fiskeeksport AS", "municipality": "NOTODDEN"}
        decision = choose_search_candidate(profile, [{
            "url": "https://norskfiskeeksport.no/",
            "title": "Norsk Fiskeeksport AS",
            "snippet": "Seafood exporter in Notodden",
            "rank": 1,
            "provider": "fixture",
        }])
        self.assertFalse(decision["abstained"])
        self.assertEqual(decision["selected"]["status"], "accepted_for_crawl")
        self.assertIn("Publication still requires", decision["policy"])

    def test_ambiguous_name_match_without_host_support_abstains(self):
        profile = {"organisation_number": "923609016", "name": "Norsk Fiskeeksport AS", "municipality": "NOTODDEN"}
        decision = choose_search_candidate(profile, [{"url": "https://parent-group.no/", "title": "Norsk Fiskeeksport AS - portfolio", "snippet": "Group companies"}])
        self.assertTrue(decision["abstained"])


class SentimentTests(unittest.TestCase):
    @staticmethod
    def item(item_id, label="positive", source="https://news.example/a", **changes):
        item = {
            "id": str(item_id), "label": label, "exact_entity": True,
            "source_class": "licensed_news", "source_url": source,
            "retrieved_at": "2026-08-22T00:00:00Z", "evidence_span": "Exact-company event sentence.",
            "content_sha256": "a" * 64,
        }
        item.update(changes)
        return item

    def test_company_owned_or_unhashed_items_are_not_publishable(self):
        self.assertFalse(publishable_sentiment_item(self.item(1, source_class="company_owned")))
        self.assertFalse(publishable_sentiment_item(self.item(1, content_sha256=None)))

    def test_inference_input_requires_exact_entity_independent_source_and_hash(self):
        item = self.item(1, text="Selskapet vant en ny kontrakt.")
        self.assertTrue(sentiment_input_eligibility(item)[0])
        accepted, reasons = sentiment_input_eligibility({**item, "exact_entity": False, "content_sha256": None})
        self.assertFalse(accepted)
        self.assertIn("exact company identity is not verified", reasons)
        self.assertIn("missing content_sha256", reasons)

    def test_pinned_model_output_normalization_is_closed_set(self):
        self.assertEqual(len(MODEL_REVISION), 40)
        self.assertEqual(normalize_generated_label(" Positive. "), "positive")
        self.assertIsNone(normalize_generated_label("bullish"))

    def test_rollup_requires_two_independent_publishers(self):
        same_publisher = [
            self.item(1, source="https://news.example/a"),
            self.item(2, source="https://news.example/b"),
        ]
        self.assertEqual(aggregate_company_sentiment(same_publisher)["status"], "abstain")
        independent = same_publisher + [self.item(3, source="https://other.example/c")]
        result = aggregate_company_sentiment(independent)
        self.assertEqual(result["status"], "available")
        self.assertEqual(result["label"], "positive")

    def test_perfect_balanced_300_item_corpus_passes_poc_gate(self):
        labels = ("positive", "neutral", "negative", "mixed")
        gold = [{"id": str(i), "label": labels[i % 4]} for i in range(300)]
        predictions = [self.item(i, labels[i % 4], source=f"https://news{i % 7}.example/item/{i}") for i in range(300)]
        report = evaluate_predictions(gold, predictions)
        self.assertEqual(report["accuracy"], 1.0)
        self.assertEqual(report["macro_f1"], 1.0)
        self.assertTrue(report["qualification_passed"])
        self.assertFalse(report["production_scale_gate_passed"])

    def test_wrong_entity_and_company_owned_predictions_fail_gate(self):
        labels = ("positive", "neutral", "negative")
        gold = [{"id": str(i), "label": labels[i % 3]} for i in range(300)]
        predictions = [self.item(i, labels[i % 3], source=f"https://news.example/{i}") for i in range(300)]
        predictions[0]["exact_entity"] = False
        predictions[1]["source_class"] = "company_owned"
        report = evaluate_predictions(gold, predictions)
        self.assertEqual(report["wrong_entity_predictions"], 1)
        self.assertEqual(report["company_owned_predictions"], 1)
        self.assertFalse(report["qualification_passed"])

    def test_qualification_minimum_cannot_be_weakened(self):
        with self.assertRaises(ValueError):
            evaluate_predictions([{"id": "1", "label": "positive"}], [self.item(1)], minimum_items=1)


class OfficialNormalizationTests(unittest.TestCase):
    def test_accounting_obligation_is_categorical_for_as_but_not_enk(self):
        company = accounting_obligation_assessment({"organisation_number": "923609016", "legal_form": "AS"})
        sole_trader = accounting_obligation_assessment({"organisation_number": "923609017", "legal_form": "ENK", "employees": 0})
        self.assertEqual(company["value"]["classification"], "required_by_legal_form")
        self.assertEqual(sole_trader["value"]["classification"], "threshold_or_activity_dependent")
        self.assertTrue(company["content_sha256"])
        self.assertEqual(company["source_row_key"], "923609016")

    def test_observed_filing_overrides_rule_path(self):
        record = accounting_obligation_assessment({"organisation_number": "923609018", "legal_form": "ENK", "latest_submitted_accounts": "2024"})
        self.assertEqual(record["value"]["classification"], "filing_observed")

    def test_entity_normalization_keeps_identity(self):
        record = normalize_entity({"organisasjonsnummer": "923609016", "navn": "EQUINOR ASA", "organisasjonsform": {"kode": "ASA"}})
        self.assertEqual(record["organisation_number"], "923609016")
        self.assertEqual(record["legal_form"], "ASA")

    def test_financial_fields_keep_period_currency_and_zero(self):
        body = [{"id": 1, "valuta": "NOK", "regnskapsperiode": {"tilDato": "2025-12-31"}, "resultatregnskapResultat": {"driftsresultat": {"driftsresultat": 0, "driftsinntekter": {"sumDriftsinntekter": 12}}}}]
        record = normalize_financials(body)["records"][0]
        self.assertEqual(record["revenue"], 12)
        self.assertEqual(record["operating_result"], 0)
        self.assertEqual(record["currency"], "NOK")

    def test_financial_history_is_sorted_and_links_to_official_pdfs(self):
        record = normalize_financial_history(["2024", "2022", "2024", "invalid"], "923609016")
        self.assertEqual(record["years"], ["2022", "2024"])
        self.assertEqual(record["pdfs"][0]["year"], "2024")
        self.assertTrue(record["pdfs"][0]["url"].endswith("/923609016/2024"))

    def test_public_roles_drop_birth_dates(self):
        body = {"rollegrupper": [{"type": {"kode": "STYR"}, "roller": [{"type": {"kode": "LEDE", "beskrivelse": "Chair"}, "person": {"fodselsdato": "1970-01-01", "navn": {"fornavn": "Ada", "etternavn": "Nord"}}}]}]}
        record = normalize_roles(body)["roles"][0]
        self.assertEqual(record["name"], "Ada Nord")
        self.assertNotIn("fodselsdato", json.dumps(record))


class ResearchAgentTests(unittest.TestCase):
    @staticmethod
    def screen_row(org, municipality, employees, revenue):
        return {
            "organisation_number": org,
            "name": f"Company {org}",
            "municipality": municipality,
            "employees": employees,
            "evidence": {
                "registry": evidence("registry", "available", "official_registry_bulk", "https://example.test/registry", content_sha256="a" * 64),
                "financials": evidence("financials", "available", "official_annual_accounts", "https://example.test/accounts", value={"records": [{"revenue": revenue, "annual_result": 1}]}, content_sha256="b" * 64),
            },
        }

    def test_cross_company_screen_has_exact_membership_and_inspectable_plan(self):
        rows = [
            self.screen_row("111111111", "OSLO", 20, 2_000_000),
            self.screen_row("222222222", "OSLO", 5, 2_000_000),
            self.screen_row("333333333", "BERGEN", 20, 2_000_000),
            self.screen_row("444444444", "OSLO", 20, 500_000),
        ]
        result = screen_profiles(rows, "companies in Oslo with more than 10 employees and revenue over 1 million")
        self.assertFalse(result["abstained"])
        self.assertEqual([item["organisation_number"] for item in result["results"]], ["111111111"])
        self.assertEqual(len(result["plan"]["filters"]), 3)
        self.assertTrue(all(citation["content_sha256"] for citation in result["results"][0]["citations"]))

    def test_unsupported_cross_company_criterion_abstains(self):
        plan = parse_screen_query("companies in Oslo with positive Glassdoor sentiment")
        self.assertFalse(plan["executable"])
        result = screen_profiles([], "companies in Oslo with positive Glassdoor sentiment")
        self.assertTrue(result["abstained"])
        self.assertIn("Glassdoor", result["reason"])

    def test_missing_website_is_not_treated_as_proven_absence(self):
        result = screen_profiles([], "companies without a website")
        self.assertTrue(result["abstained"])
        self.assertIn("does not prove", result["reason"])

    def test_workspace_saves_pins_history_and_recovers_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workspace.json"
            workspace, warning = load_workspace(path)
            self.assertIsNone(warning)
            updated = record_screen(workspace, {"query": "in Oslo", "plan": {"filters": []}, "result_count": 1, "results": [{"organisation_number": "111111111"}]}, pin_organisations=["111111111"])
            save_workspace(path, updated)
            loaded, warning = load_workspace(path)
            self.assertEqual(loaded["pins"], ["111111111"])
            self.assertEqual(len(loaded["history"]), 1)
            path.write_text("not json", encoding="utf-8")
            recovered, warning = load_workspace(path)
            self.assertEqual(recovered["pins"], [])
            self.assertIn("recovery", warning or "")

    def test_sentiment_abstains_and_every_fact_has_a_source(self):
        row = {
            "organisation_number": "923609016", "name": "Example AS", "legal_form": "AS",
            "municipality": "OSLO", "employees": None,
            "evidence": {"registry": evidence("registry", "available", "official", "https://example.test")},
        }
        result = answer_profile(row, "Give me employee sentiment")
        self.assertTrue(any("Sentiment is not scored" in item for item in result["unsupported_or_uncertain"]))
        self.assertTrue(all(fact["source_url"] for fact in result["facts"]))
        self.assertFalse(any(fact["claim"] == "Registry employee count" for fact in result["facts"]))

    def test_natural_leads_word_routes_to_roles(self):
        roles = evidence("roles", "available", "official", "https://example.test/roles", value={"roles": [{"name": "Ada Nord", "role": "Chair", "inactive": False}]})
        row = {"organisation_number": "923609016", "name": "Example AS", "evidence": {"roles": roles}}
        result = answer_profile(row, "Who leads this company?")
        self.assertEqual(result["facts"][0]["value"], "Ada Nord")

    def test_quarantined_website_claims_are_not_returned(self):
        website = evidence("website", "available", "company_site", "https://parent.test", value={"description": "Parent claim", "social_links": [{"platform": "linkedin", "url": "https://linkedin.com/company/parent"}], "identity_assessment": {"publishable": False}})
        row = {"organisation_number": "923609016", "name": "Subsidiary AS", "evidence": {"website": website}}
        result = answer_profile(row, "What social information is available?")
        self.assertFalse(result["facts"])
        self.assertTrue(any("quarantined" in item for item in result["unsupported_or_uncertain"]))


class PrototypeTests(unittest.TestCase):
    def test_qualification_copy_keeps_production_boundary_visible(self):
        header, boundary = qualification_copy({
            "weighted_score": {"verified_points": 100, "maximum_points": 100},
            "qualification": {"poc_qualified": True, "production_qualified": False},
        })
        self.assertIn("100/100", header)
        self.assertIn("not production-qualified", header)
        self.assertIn("remain quarantined", boundary)

    def test_quarantined_social_links_are_counted_but_not_published(self):
        row = {
            "organisation_number": "923609016",
            "name": "Subsidiary AS",
            "legal_form": "AS",
            "employees": None,
            "municipality": "OSLO",
            "industry_code": None,
            "industry_label": None,
            "website": "https://parent.test",
            "bankrupt": False,
            "liquidating": False,
            "evidence": {
                "website": evidence(
                    "website",
                    "available",
                    "company_site",
                    "https://parent.test",
                    value={
                        "identity_assessment": {"publishable": False},
                        "social_links": [],
                        "discovered_social_links": [
                            {"platform": "linkedin", "url": "https://linkedin.com/company/parent"}
                        ],
                    },
                )
            },
        }
        website = compact_prototype(row)["web"]["value"]
        self.assertEqual(website["quarantined_social_count"], 1)
        self.assertEqual(website["social_links"], [])


class RefreshTests(unittest.TestCase):
    def test_snapshot_fetcher_hashes_evaluator_bytes_and_carries_times(self):
        url = "https://example.test/entity/1"
        fetcher = SnapshotFetcher({"retrieved_at": "2026-01-02T00:00:00Z", "effective_at": "2026-01-01T00:00:00Z", "responses": {url: {"body": {"value": 1}}}})
        result = fetcher(url)
        self.assertEqual(result.status, 200)
        self.assertEqual(len(result.content_sha256 or ""), 64)
        self.assertEqual(result.effective_at, "2026-01-01T00:00:00Z")

    def test_identical_refresh_is_an_idempotent_noop(self):
        row = {"organisation_number": "923609016", "name": "Example AS", "employees": 4}
        self.assertEqual(diff_profile(row, dict(row)), [])

    def test_missing_to_zero_is_a_real_change_with_provenance(self):
        source = evidence("registry", "available", "official", "https://example.test/entity")
        old = {"organisation_number": "923609016", "employees": None, "evidence": {"registry": source}}
        new = {"organisation_number": "923609016", "employees": 0, "evidence": {"registry": source}}
        change = diff_profile(old, new)[0]
        self.assertIsNone(change["old_value"])
        self.assertEqual(change["new_value"], 0)
        self.assertEqual(change["source_url"], "https://example.test/entity")

    def test_refresh_rejects_membership_or_identity_drift(self):
        with self.assertRaises(ValueError):
            diff_datasets([{"organisation_number": "923609016"}], [{"organisation_number": "999999999"}])


class WebsiteIdentityTests(unittest.TestCase):
    def test_group_contact_page_listing_subsidiary_org_number_is_not_exact_homepage_identity(self):
        profile = {
            "organisation_number": "915637353",
            "name": "SKS PRODUKSJON AS",
            "evidence": {"website": evidence("website", "available", "company_site", "https://sks.no", value={
                "final_url": "https://sks.no/",
                "title": "Konsern - SKS - Forside",
                "main_text_excerpt": "SKS is a power group with multiple subsidiaries.",
                "pages": [{"title": "Contact", "main_text_excerpt": "SKS Produksjon AS organisation number 915 637 353"}],
            })},
        }
        assessment = assess_website_identity(profile)
        self.assertFalse(assessment["publishable"])
        self.assertNotEqual(assessment["score"], 1.0)

    def test_shared_identity_gate_quarantines_parent_social_links(self):
        website = evidence("website", "available", "company_site", "https://parent.test", value={
            "title": "Parent Group", "main_text_excerpt": "Parent Group portfolio",
            "social_links": [{"platform": "linkedin", "url": "https://linkedin.com/company/parent"}],
        })
        profile = {"organisation_number": "923609016", "name": "Exact Subsidiary AS", "evidence": {}}
        result = apply_website_identity_gate(profile, website)
        self.assertFalse(result["assessment"]["publishable"])
        self.assertEqual(result["website"]["value"]["social_links"], [])
        self.assertEqual(result["quarantined_social_links"], 1)

    def test_exact_legal_name_is_publishable(self):
        row = {"organisation_number": "923609016", "name": "Norsk Fiskeeksport AS", "evidence": {"website": {"status": "available", "value": {"title": "Norsk Fiskeeksport AS"}}}}
        self.assertTrue(assess_website_identity(row)["publishable"])

    def test_parent_brand_without_legal_name_is_quarantined(self):
        row = {"organisation_number": "988412406", "name": "Tevlingveien 23 Invest AS", "evidence": {"website": {"status": "available", "value": {"title": "Ragde Eiendom"}}}}
        self.assertFalse(assess_website_identity(row)["publishable"])

    def test_parked_domain_and_parent_company_sports_site_are_quarantined(self):
        parked = {"organisation_number": "996081001", "name": "Condalign AS", "evidence": {"website": {"status": "available", "value": {"title": "CondAlign.com is for sale | HugeDomains"}}}}
        sports = {"organisation_number": "996242692", "name": "Primulator B.I.L.", "evidence": {"website": {"status": "available", "value": {"title": "Primulator", "description": "Premium products for HoReCa"}}}}
        self.assertFalse(assess_website_identity(parked)["publishable"])
        self.assertFalse(assess_website_identity(sports)["publishable"])

    def test_hosting_placeholder_and_generic_link_page_are_quarantined(self):
        hosting = {"organisation_number": "917568278", "name": "HJELMEN AS", "evidence": {"website": {"status": "available", "value": {"title": "www.Hjelmen-as.no is parked at Miss Hosting Web Hosting", "main_text_excerpt": "Hjelmen " * 100}}}}
        links = {"organisation_number": "986606009", "name": "KOALA ANS", "evidence": {"website": {"status": "available", "value": {"title": "koala.no", "description": "Find the best information and most relevant links on all topics related to"}}}}
        self.assertFalse(assess_website_identity(hosting)["publishable"])
        self.assertFalse(assess_website_identity(links)["publishable"])

    def test_broader_umbrella_site_is_not_exact_when_name_only_appears_in_body(self):
        row = {
            "organisation_number": "976994027",
            "name": "AVALDSNES SOKN",
            "evidence": {"website": {"status": "available", "value": {
                "title": "Kirken i Karmøy",
                "final_url": "https://www.karmoykirken.no/",
                "main_text_excerpt": "Avaldsnes sokn is one of several parishes represented on this umbrella site.",
            }}},
        }
        self.assertFalse(assess_website_identity(row)["publishable"])

    def test_social_handle_requires_exact_entity_name_evidence(self):
        aon = {"name": "Aon Norway AS"}
        fish = {"name": "Norsk Fiskeeksport AS"}
        self.assertFalse(assess_social_identity(aon, {"platform": "linkedin", "url": "https://linkedin.com/company/aon"})["publishable"])
        self.assertTrue(assess_social_identity(fish, {"platform": "linkedin", "url": "https://linkedin.com/company/norsk-fiskeeksport"})["publishable"])


class VerifiedSiteSeedTests(unittest.TestCase):
    def test_verified_seed_is_applied_and_unknown_org_is_rejected(self):
        import subprocess
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profiles = root / "profiles.jsonl"
            seeds = root / "seeds.json"
            output = root / "output.jsonl"
            report = root / "report.json"
            profiles.write_text(json.dumps({"organisation_number": "123456789", "name": "Example AS"}) + "\n")
            seeds.write_text(json.dumps([{
                "organisation_number": "123456789",
                "website": "https://example.no/",
                "proof_url": "https://source.example/proof",
                "proof": "Exact name and organisation number",
            }]))
            command = [
                sys.executable, str(ROOT / "scripts" / "apply_verified_site_seeds.py"),
                "--profiles", str(profiles), "--seeds", str(seeds),
                "--output", str(output), "--report", str(report),
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)
            row = json.loads(output.read_text().strip())
            self.assertEqual(row["website"], "https://example.no/")
            self.assertEqual(row["website_seed_source"], "independently_verified_exact_entity")
            self.assertEqual(json.loads(report.read_text())["applied"], 1)

            seeds.write_text(json.dumps([{
                "organisation_number": "987654321",
                "website": "https://unknown.no/",
                "proof_url": "https://source.example/proof",
            }]))
            failed = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(failed.returncode, 0)
            self.assertIn("unknown organisations", failed.stderr)


class ResearchWebsiteSafetyTests(unittest.TestCase):
    def _profile(self, assessment, organisation_number="923609016"):
        value = {"description": "A family-run workshop in Oslo.", "social_links": [{"platform": "linkedin", "url": "https://www.linkedin.com/company/example-as"}]}
        if assessment is not None:
            value["identity_assessment"] = assessment
        return {
            "organisation_number": organisation_number,
            "name": "EXAMPLE AS",
            "municipality": "OSLO",
            "evidence": {"website": {"status": "available", "value": value}},
        }

    def test_missing_identity_assessment_keeps_website_claims_out_of_the_answer(self):
        result = answer_profile(self._profile(None), "social")
        self.assertEqual(result["facts"], [])
        self.assertTrue(any("quarantined" in message for message in result["unsupported_or_uncertain"]))

    def test_explicitly_publishable_identity_allows_website_and_social_claims(self):
        result = answer_profile(self._profile({"publishable": True}), "social")
        self.assertEqual(len(result["facts"]), 2)
        self.assertEqual(result["facts"][0]["claim"], "Website description")
        self.assertEqual(result["facts"][1]["claim"], "Declared linkedin profile")
        self.assertEqual(result["unsupported_or_uncertain"], [])

    def test_explicitly_non_publishable_identity_quarantines_website_claims(self):
        result = answer_profile(self._profile({"publishable": False}), "social")
        self.assertEqual(result["facts"], [])
        self.assertTrue(any("quarantined" in message for message in result["unsupported_or_uncertain"]))

    def test_screen_website_filter_requires_a_publishable_identity_assessment(self):
        rows = [
            self._profile({"publishable": True}, organisation_number="111111111"),
            self._profile(None, organisation_number="222222222"),
            self._profile({"publishable": False}, organisation_number="333333333"),
        ]
        result = screen_profiles(rows, "companies in Oslo with a website")
        self.assertFalse(result["abstained"])
        self.assertEqual(result["result_count"], 1)
        self.assertEqual(result["results"][0]["organisation_number"], "111111111")


class _SupportsRaisesConnector(MockConnector):
    def supports(self, task):
        raise RuntimeError("supports exploded")


class _EstimateRaisesConnector(MockConnector):
    def estimate_requests(self, task):
        raise ValueError("estimate exploded")


class _UncoercibleEstimateConnector(MockConnector):
    def estimate_requests(self, task):
        return "not-a-number"


class _AcquisitionMetadataRaisesConnector(MockConnector):
    def __init__(self, **kwargs):
        self._acquisition_mode = "official_api"
        super().__init__(**kwargs)

    @property
    def acquisition_mode(self):
        raise ZeroDivisionError("acquisition metadata exploded")

    @acquisition_mode.setter
    def acquisition_mode(self, value):
        self._acquisition_mode = value


class _RightsMetadataRaisesConnector(MockConnector):
    def __init__(self, **kwargs):
        self._rights_status = "approved"
        super().__init__(**kwargs)

    @property
    def rights_status(self):
        raise ZeroDivisionError("rights metadata exploded")

    @rights_status.setter
    def rights_status(self, value):
        self._rights_status = value


class ExternalConnectorExecutorTests(unittest.TestCase):
    def setUp(self):
        self.now = "2026-01-15T09:00:00Z"
        self.previous_guard = os.environ.get("SIGNALPOST_ENABLE_TEST_CONNECTORS")
        os.environ["SIGNALPOST_ENABLE_TEST_CONNECTORS"] = "1"

    def tearDown(self):
        if self.previous_guard is None:
            os.environ.pop("SIGNALPOST_ENABLE_TEST_CONNECTORS", None)
        else:
            os.environ["SIGNALPOST_ENABLE_TEST_CONNECTORS"] = self.previous_guard

    def _budget(self, **kwargs):
        return RequestBudget(kwargs.pop("max_requests", 20), **kwargs)

    def test_unknown_connector_is_an_explicit_failure_not_a_silent_skip(self):
        result = run_external_tasks(
            [mock_task(connector="absent_connector")], registry=default_registry(), budget=self._budget(), now=self.now
        )
        self.assertEqual(result["planned_tasks"], 1)
        self.assertEqual(result["completed_tasks"], 0)
        self.assertEqual(result["observations"], [])
        self.assertEqual([item["reason"] for item in result["failures"]], [FAILURE_UNKNOWN_CONNECTOR])
        self.assertEqual(result["failures"][0]["state"], NOT_AVAILABLE)
        self.assertEqual(result["failures"][0]["connector"], "absent_connector")

    def test_unapproved_acquisition_mode_is_refused_before_any_request(self):
        registry = mock_registry(MockConnector(acquisition_mode="web_scrape"))
        budget = self._budget()
        result = run_external_tasks([mock_task()], registry=registry, budget=budget, now=self.now)
        self.assertEqual([item["reason"] for item in result["failures"]], [FAILURE_UNSUPPORTED_ACQUISITION_MODE])
        self.assertEqual(result["observations"], [])
        self.assertEqual(budget.total_requests, 0)

    def test_unapproved_rights_status_is_refused_before_any_request(self):
        registry = mock_registry(MockConnector(rights_status="review_required"))
        budget = self._budget()
        result = run_external_tasks([mock_task()], registry=registry, budget=budget, now=self.now)
        self.assertEqual([item["reason"] for item in result["failures"]], [FAILURE_RIGHTS_NOT_APPROVED])
        self.assertEqual(result["observations"], [])
        self.assertEqual(budget.total_requests, 0)

    def test_unsupported_task_type_is_refused_before_any_request(self):
        registry = mock_registry(MockConnector(supported_task_types={"discover_independent_mentions"}))
        result = run_external_tasks([mock_task()], registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual([item["reason"] for item in result["failures"]], [FAILURE_UNSUPPORTED_TASK_TYPE])
        self.assertEqual(result["observations"], [])

    def test_unverified_identity_observation_is_rejected_and_never_publishes(self):
        registry = mock_registry(MockConnector())
        result = run_external_tasks([mock_task(mode="unverified")], registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["observations"], [])
        self.assertEqual(len(result["rejected_observations"]), 1)
        reasons = result["rejected_observations"][0]["reasons"]
        self.assertIn("exact legal entity is not verified", reasons)
        self.assertIn("missing exact-entity proof", reasons)

    def test_ambiguous_identity_observation_is_rejected_and_never_publishes(self):
        registry = mock_registry(MockConnector())
        result = run_external_tasks([mock_task(mode="ambiguous")], registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["observations"], [])
        self.assertEqual(len(result["rejected_observations"]), 1)
        self.assertIn("exact legal entity is not verified", result["rejected_observations"][0]["reasons"])

    def test_unapproved_acquisition_mode_in_an_observation_is_rejected(self):
        registry = mock_registry(MockConnector())
        result = run_external_tasks([mock_task(mode="unknown_acquisition")], registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["observations"], [])
        self.assertIn("acquisition mode is not approved for publication", result["rejected_observations"][0]["reasons"])

    def test_unapproved_rights_in_an_observation_is_rejected(self):
        registry = mock_registry(MockConnector())
        result = run_external_tasks([mock_task(mode="unapproved_rights")], registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["observations"], [])
        self.assertIn("source rights are not approved", result["rejected_observations"][0]["reasons"])

    def test_verified_observation_is_published_with_executor_owned_provenance(self):
        registry = mock_registry(MockConnector())
        result = run_external_tasks([mock_task()], registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["completed_tasks"], 1)
        self.assertEqual(result["rejected_observations"], [])
        observation = result["observations"][0]
        self.assertEqual(observation["task_id"], mock_task()["task_id"])
        self.assertEqual(observation["connector"], "mock_places")
        self.assertEqual(observation["organisation_number"], "923609016")
        self.assertEqual(observation["retrieved_at"], self.now)
        self.assertEqual(validate_observation(observation), [])

    def test_observation_for_another_organisation_is_rejected_on_identity(self):
        registry = mock_registry(MockConnector())
        result = run_external_tasks([mock_task(mode="cross_organisation")], registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["observations"], [])
        self.assertEqual(len(result["rejected_observations"]), 1)
        self.assertEqual(result["rejected_observations"][0]["reasons"], ["organisation number does not match the planned task"])
        self.assertNotIn("source_url", result["rejected_observations"][0])

    def test_request_cap_denial_is_recorded_as_budget_failure(self):
        registry = mock_registry(MockConnector())
        tasks = [mock_task(extra={"mock_requests": 2}), mock_task(extra={"mock_requests": 2})]
        result = run_external_tasks(tasks, registry=registry, budget=self._budget(max_requests=3), now=self.now)
        self.assertEqual(result["failures"][-1]["reason"], FAILURE_BUDGET_DENIED)
        self.assertEqual(result["failures"][-1]["state"], FAILED)
        self.assertEqual(result["budget"]["denied_requests"], 2)
        self.assertEqual(result["budget"]["total_requests"], 2)
        self.assertEqual(result["budget"]["actual_requests"], 2)
        self.assertEqual(len(result["observations"]), 1)

    def test_cost_cap_denial_is_recorded_as_budget_failure(self):
        registry = mock_registry(MockConnector(cost_per_request=1.5))
        tasks = [mock_task(extra={"mock_requests": 2}), mock_task(extra={"mock_requests": 2})]
        result = run_external_tasks(tasks, registry=registry, budget=self._budget(max_cost=4.0), now=self.now)
        self.assertEqual(result["failures"][-1]["reason"], FAILURE_BUDGET_DENIED)
        self.assertEqual(result["budget"]["denied_cost"], 3.0)
        self.assertEqual(result["budget"]["actual_cost"], 3.0)

    def test_connector_failure_is_reported_with_reason_and_state(self):
        registry = mock_registry(MockConnector())
        result = run_external_tasks([mock_task(mode="fail")], registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["failures"][0]["state"], FAILED)
        self.assertEqual(result["failures"][0]["reason"], "mock connector failed on record")
        self.assertEqual(result["failures"][0]["connector"], "mock_places")
        self.assertEqual(result["observations"], [])

    def test_connector_missing_record_is_not_available_not_failed(self):
        registry = mock_registry(MockConnector())
        result = run_external_tasks([mock_task(mode="not_available")], registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["failures"][0]["state"], NOT_AVAILABLE)
        self.assertEqual(result["failures"][0]["reason"], "mock source reports no record")
        self.assertEqual(result["observations"], [])

    def test_raising_connector_is_contained_and_never_takes_down_the_batch(self):
        registry = mock_registry(MockConnector())
        tasks = [mock_task(mode="raise_error"), mock_task()]
        result = run_external_tasks(tasks, registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["planned_tasks"], 2)
        self.assertEqual(result["completed_tasks"], 1)
        self.assertEqual(result["failures"][0]["reason"], "RuntimeError: mock connector raised")
        self.assertEqual(result["failures"][0]["state"], FAILED)
        self.assertEqual(len(result["observations"]), 1)

    def test_support_check_exception_becomes_a_structured_failure(self):
        task = mock_task(connector="mock_supports_raises")
        registry = mock_registry(_SupportsRaisesConnector(name="mock_supports_raises"))
        budget = self._budget()
        result = run_external_tasks([task], registry=registry, budget=budget, now=self.now)
        self.assertEqual(result["planned_tasks"], 1)
        self.assertEqual(result["completed_tasks"], 0)
        self.assertEqual(result["failed_tasks"], 1)
        self.assertEqual(result["completed_tasks"] + result["failed_tasks"], result["planned_tasks"])
        self.assertEqual(result["observations"], [])
        self.assertEqual(result["rejected_observations"], [])
        self.assertEqual(len(result["failures"]), 1)
        failure = result["failures"][0]
        self.assertEqual(failure["task_id"], task["task_id"])
        self.assertEqual(failure["connector"], "mock_supports_raises")
        self.assertEqual(failure["organisation_number"], "923609016")
        self.assertEqual(failure["state"], FAILED)
        self.assertEqual(failure["stage"], STAGE_SUPPORT_CHECK)
        self.assertEqual(failure["reason"], "RuntimeError: supports exploded")
        self.assertEqual(budget.total_requests, 0)

    def test_estimate_requests_exception_becomes_a_structured_failure(self):
        task = mock_task(connector="mock_estimate_raises")
        registry = mock_registry(_EstimateRaisesConnector(name="mock_estimate_raises"))
        budget = self._budget()
        result = run_external_tasks([task], registry=registry, budget=budget, now=self.now)
        self.assertEqual(result["planned_tasks"], 1)
        self.assertEqual(result["completed_tasks"], 0)
        self.assertEqual(result["failed_tasks"], 1)
        self.assertEqual(result["completed_tasks"] + result["failed_tasks"], result["planned_tasks"])
        self.assertEqual(result["observations"], [])
        self.assertEqual(len(result["failures"]), 1)
        failure = result["failures"][0]
        self.assertEqual(failure["task_id"], task["task_id"])
        self.assertEqual(failure["connector"], "mock_estimate_raises")
        self.assertEqual(failure["state"], FAILED)
        self.assertEqual(failure["stage"], STAGE_REQUEST_ESTIMATION)
        self.assertEqual(failure["reason"], "ValueError: estimate exploded")
        self.assertEqual(budget.total_requests, 0)

    def test_estimate_coercion_exception_becomes_a_structured_failure(self):
        task = mock_task(connector="mock_uncoercible_estimate")
        registry = mock_registry(_UncoercibleEstimateConnector(name="mock_uncoercible_estimate"))
        budget = self._budget()
        result = run_external_tasks([task], registry=registry, budget=budget, now=self.now)
        self.assertEqual(result["planned_tasks"], 1)
        self.assertEqual(result["completed_tasks"], 0)
        self.assertEqual(result["failed_tasks"], 1)
        self.assertEqual(result["completed_tasks"] + result["failed_tasks"], result["planned_tasks"])
        self.assertEqual(result["observations"], [])
        self.assertEqual(len(result["failures"]), 1)
        failure = result["failures"][0]
        self.assertEqual(failure["task_id"], task["task_id"])
        self.assertEqual(failure["connector"], "mock_uncoercible_estimate")
        self.assertEqual(failure["state"], FAILED)
        self.assertEqual(failure["stage"], STAGE_ESTIMATE_COERCION)
        self.assertIn("invalid literal for int()", failure["reason"])
        self.assertEqual(budget.total_requests, 0)

    def test_execute_exception_records_the_execution_stage(self):
        task = mock_task(mode="raise_error")
        registry = mock_registry(MockConnector())
        result = run_external_tasks([task], registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["completed_tasks"], 0)
        self.assertEqual(result["failed_tasks"], 1)
        self.assertEqual(result["observations"], [])
        failure = result["failures"][0]
        self.assertEqual(failure["task_id"], task["task_id"])
        self.assertEqual(failure["connector"], "mock_places")
        self.assertEqual(failure["state"], FAILED)
        self.assertEqual(failure["stage"], STAGE_EXECUTION)
        self.assertEqual(failure["reason"], "RuntimeError: mock connector raised")

    def test_one_raising_connector_task_does_not_stop_the_remaining_tasks(self):
        registry = mock_registry(_EstimateRaisesConnector(name="mock_estimate_raises"), MockConnector())
        tasks = [
            mock_task(connector="mock_places"),
            mock_task(connector="mock_estimate_raises"),
            mock_task(connector="mock_places", mode="unverified"),
            mock_task(connector="mock_places", purpose="discover_active_jobs"),
        ]
        result = run_external_tasks(tasks, registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["planned_tasks"], 4)
        self.assertEqual(result["completed_tasks"], 3)
        self.assertEqual(result["failed_tasks"], 1)
        self.assertEqual(result["completed_tasks"] + result["failed_tasks"], result["planned_tasks"])
        self.assertEqual(len(result["observations"]), 2)
        self.assertEqual(len(result["rejected_observations"]), 1)
        self.assertEqual(len(result["failures"]), 1)
        self.assertEqual(result["failures"][0]["connector"], "mock_estimate_raises")
        self.assertEqual(result["failures"][0]["stage"], STAGE_REQUEST_ESTIMATION)
        self.assertEqual({item["organisation_number"] for item in result["observations"]}, {"923609016"})

    def test_acquisition_metadata_property_exception_becomes_a_structured_failure(self):
        task = mock_task(connector="mock_acquisition_raises")
        registry = mock_registry(_AcquisitionMetadataRaisesConnector(name="mock_acquisition_raises"))
        budget = self._budget()
        result = run_external_tasks([task], registry=registry, budget=budget, now=self.now)
        self.assertEqual(result["planned_tasks"], 1)
        self.assertEqual(result["completed_tasks"], 0)
        self.assertEqual(result["failed_tasks"], 1)
        self.assertEqual(result["completed_tasks"] + result["failed_tasks"], result["planned_tasks"])
        self.assertEqual(result["observations"], [])
        self.assertEqual(len(result["failures"]), 1)
        failure = result["failures"][0]
        self.assertEqual(failure["task_id"], task["task_id"])
        self.assertEqual(failure["connector"], "mock_acquisition_raises")
        self.assertEqual(failure["organisation_number"], "923609016")
        self.assertEqual(failure["state"], FAILED)
        self.assertEqual(failure["stage"], STAGE_ACQUISITION_GATE)
        self.assertEqual(failure["reason"], "ZeroDivisionError: acquisition metadata exploded")
        self.assertEqual(budget.total_requests, 0)

    def test_rights_metadata_property_exception_becomes_a_structured_failure(self):
        task = mock_task(connector="mock_rights_raises")
        registry = mock_registry(_RightsMetadataRaisesConnector(name="mock_rights_raises"))
        budget = self._budget()
        result = run_external_tasks([task], registry=registry, budget=budget, now=self.now)
        self.assertEqual(result["planned_tasks"], 1)
        self.assertEqual(result["completed_tasks"], 0)
        self.assertEqual(result["failed_tasks"], 1)
        self.assertEqual(result["completed_tasks"] + result["failed_tasks"], result["planned_tasks"])
        self.assertEqual(result["observations"], [])
        self.assertEqual(len(result["failures"]), 1)
        failure = result["failures"][0]
        self.assertEqual(failure["task_id"], task["task_id"])
        self.assertEqual(failure["connector"], "mock_rights_raises")
        self.assertEqual(failure["state"], FAILED)
        self.assertEqual(failure["stage"], STAGE_RIGHTS_GATE)
        self.assertEqual(failure["reason"], "ZeroDivisionError: rights metadata exploded")
        self.assertEqual(budget.total_requests, 0)

    def test_malformed_non_dict_task_becomes_a_structured_failure(self):
        result = run_external_tasks(["not-a-dict"], registry=mock_registry(), budget=self._budget(), now=self.now)
        self.assertEqual(result["planned_tasks"], 1)
        self.assertEqual(result["completed_tasks"], 0)
        self.assertEqual(result["failed_tasks"], 1)
        self.assertEqual(result["completed_tasks"] + result["failed_tasks"], result["planned_tasks"])
        self.assertEqual(result["observations"], [])
        self.assertEqual(len(result["failures"]), 1)
        failure = result["failures"][0]
        self.assertEqual(failure["task_id"], json.dumps("not-a-dict", ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        self.assertIsNone(failure["connector"])
        self.assertIsNone(failure["organisation_number"])
        self.assertEqual(failure["state"], FAILED)
        self.assertEqual(failure["stage"], STAGE_TASK_VALIDATION)
        self.assertEqual(failure["reason"], FAILURE_INVALID_TASK)

    def test_mixed_malformed_and_raising_tasks_leave_every_planned_task_with_an_outcome(self):
        registry = mock_registry(_AcquisitionMetadataRaisesConnector(name="mock_acquisition_raises"), MockConnector())
        tasks = [
            "not-a-dict",
            mock_task(connector="mock_acquisition_raises"),
            mock_task(connector="mock_places"),
            mock_task(connector="mock_places", purpose="discover_active_jobs"),
        ]
        result = run_external_tasks(tasks, registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["planned_tasks"], 4)
        self.assertEqual(result["completed_tasks"], 2)
        self.assertEqual(result["failed_tasks"], 2)
        self.assertEqual(result["completed_tasks"] + result["failed_tasks"], result["planned_tasks"])
        self.assertEqual(len(result["observations"]), 2)
        self.assertEqual(len(result["failures"]), 2)
        self.assertEqual({item["stage"] for item in result["failures"]}, {STAGE_TASK_VALIDATION, STAGE_ACQUISITION_GATE})
        self.assertEqual({item["organisation_number"] for item in result["observations"]}, {"923609016"})

    def test_no_planned_task_is_silently_dropped(self):
        registry = mock_registry(MockConnector())
        tasks = [mock_task(mode="fail"), mock_task(mode="unverified"), mock_task()]
        result = run_external_tasks(tasks, registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(result["planned_tasks"], 3)
        self.assertEqual(result["completed_tasks"] + result["failed_tasks"], 3)
        self.assertEqual(result["failed_tasks"], 1)
        self.assertEqual(len(result["rejected_observations"]), 1)
        self.assertEqual(len(result["observations"]), 1)
        self.assertTrue(result["rejected_observations"][0]["reasons"])

    def test_execution_is_deterministic_for_a_fixed_timestamp(self):
        registry = mock_registry(MockConnector())
        tasks = [mock_task(mode="unverified"), mock_task(), mock_task(mode="fail")]
        first = run_external_tasks(tasks, registry=registry, budget=self._budget(), now=self.now)
        second = run_external_tasks(tasks, registry=registry, budget=self._budget(), now=self.now)
        self.assertEqual(first["observations"], second["observations"])
        self.assertEqual(first["failures"], second["failures"])
        self.assertEqual(first["rejected_observations"], second["rejected_observations"])

    def test_default_registry_never_contains_test_connectors(self):
        self.assertEqual(len(default_registry()), 0)
        self.assertNotIn("mock_places", default_registry())

    def test_mock_registry_refuses_to_load_without_the_explicit_test_guard(self):
        os.environ.pop("SIGNALPOST_ENABLE_TEST_CONNECTORS", None)
        with self.assertRaises(RuntimeError):
            mock_registry()
        os.environ["SIGNALPOST_ENABLE_TEST_CONNECTORS"] = "1"
        self.assertIn("mock_places", mock_registry())


class ExternalStepTerminalGateTests(unittest.TestCase):
    def setUp(self):
        self.now = "2026-01-15T09:00:00Z"
        self.previous_guard = os.environ.get("SIGNALPOST_ENABLE_TEST_CONNECTORS")
        os.environ["SIGNALPOST_ENABLE_TEST_CONNECTORS"] = "1"

    def tearDown(self):
        if self.previous_guard is None:
            os.environ.pop("SIGNALPOST_ENABLE_TEST_CONNECTORS", None)
        else:
            os.environ["SIGNALPOST_ENABLE_TEST_CONNECTORS"] = self.previous_guard

    def _planned_registry(self, mode):
        purposes = {
            "resolve_places_and_public_rating",
            "discover_independent_mentions",
            "discover_active_jobs",
            "discover_social_handles",
        }
        return mock_registry(*[
            MockConnector(name=name, supported_task_types=purposes, default_mode=mode)
            for name in ("google_places_api", "licensed_news_search", "jobs_provider", "permitted_search_api")
        ])

    def _step(self, mode):
        profile = {"organisation_number": "923609016", "name": "EXAMPLE AS"}
        step = run_external_step(profile, registry=self._planned_registry(mode), budget=RequestBudget(20), now=self.now)
        profile["external_observations"] = step["observations"]
        profile["external_step"] = {key: value for key, value in step.items() if key != "observations"}
        return profile, step

    def test_unverified_observations_never_reach_terminal_output(self):
        profile, step = self._step("unverified")
        self.assertEqual(profile["external_observations"], [])
        self.assertEqual(step["rejected_count"], 4)
        envelope = terminal_envelope(profile, run_id="test-run", modules=sorted(profile.get("evidence", {})), started_at=self.now, completed_at=self.now)
        self.assertEqual(envelope["profile"]["external_observations"], [])
        serialized = json.dumps(envelope, ensure_ascii=False)
        self.assertNotIn("https://example.test/places/123456789", serialized)
        self.assertNotIn("4.5", serialized)
        for rejection in step["rejections"]:
            self.assertIn("exact legal entity is not verified", rejection["reasons"])
            self.assertNotIn("source_url", rejection)
            self.assertNotIn("content_sha256", rejection)
            self.assertNotIn("identity_proof", rejection)
            self.assertIn(rejection["id"], serialized)

    def test_ambiguous_observations_never_reach_terminal_output(self):
        profile, step = self._step("ambiguous")
        self.assertEqual(profile["external_observations"], [])
        self.assertEqual(step["rejected_count"], 4)
        self.assertTrue(all("exact legal entity is not verified" in item["reasons"] for item in step["rejections"]))

    def test_only_publishable_observations_are_serialized_into_the_terminal_envelope(self):
        profile, step = self._step("success")
        self.assertEqual(step["rejected_count"], 0)
        self.assertEqual(step["failures"], [])
        self.assertEqual(len(profile["external_observations"]), 4)
        for observation in profile["external_observations"]:
            self.assertEqual(validate_observation(observation), [])
            self.assertTrue(publishable_observation(observation))
        envelope = terminal_envelope(
            profile,
            run_id="test-run",
            modules=sorted(profile.get("evidence", {})),
            started_at=self.now,
            completed_at=self.now,
        )
        self.assertEqual(envelope["state"], "complete")
        self.assertEqual(len(envelope["profile"]["external_observations"]), 4)
        for observation in profile["external_observations"]:
            self.assertIn(observation["id"], json.dumps(envelope, ensure_ascii=False))

    def test_unverified_step_never_serializes_its_evidence_payload(self):
        profile, step = self._step("unverified")
        self.assertEqual(step["observations"], [])
        self.assertEqual({item["connector"] for item in step["rejections"]}, {"google_places_api", "licensed_news_search", "jobs_provider", "permitted_search_api"})
        envelope = terminal_envelope(profile, run_id="test-run", modules=sorted(profile.get("evidence", {})), started_at=self.now, completed_at=self.now)
        self.assertNotIn("https://example.test/places/123456789", json.dumps(envelope, ensure_ascii=False))


class IdentityTriangulationTests(unittest.TestCase):
    """Phase 2: an external candidate must be tied to the exact target, not merely similar."""

    target = {
        "organisation_number": "923609016",
        "name": "Nordic Signal AS",
        "domain": "example.no",
        "address": {"street": "Karl Johans gate 1", "postcode": "0159", "city": "Oslo", "municipality": "Oslo"},
        "municipality": "Oslo",
    }

    def decide(self, candidate, target=None):
        return triangulate_identity(self.target if target is None else target, candidate)

    def test_all_four_decision_states_exist(self):
        self.assertEqual(IDENTITY_DECISIONS, (EXACT, AMBIGUOUS, MISMATCHED, UNVERIFIED))

    def test_foreign_organisation_number_is_mismatched_and_can_never_publish(self):
        for declared in ("111222333", "923609017", "999999999"):
            with self.subTest(declared=declared):
                decision = self.decide(
                    {
                        "organisation_number": declared,
                        "name": "Nordic Signal AS",
                        "source_url": "https://example.no/about",
                        "address": "Karl Johans gate 1, 0159 Oslo",
                    }
                )
                self.assertEqual(decision["status"], MISMATCHED)
                self.assertFalse(decision["publishable"])

    def test_organisation_number_conflict_is_symmetric(self):
        decision = triangulate_identity(
            {"organisation_number": "111222333", "name": "Nordic Signal AS", "domain": "example.no"},
            {"organisation_number": "923609016", "source_url": "https://example.no/"},
        )
        self.assertEqual(decision["status"], MISMATCHED)
        self.assertFalse(decision["publishable"])

    def test_matching_organisation_number_is_the_canonical_anchor(self):
        decision = self.decide({"organisation_number": "923609016"})
        self.assertEqual(decision["status"], EXACT)
        self.assertTrue(decision["publishable"])
        self.assertTrue(decision["signals"]["organisation_number_match"])

    def test_exact_name_and_domain_match_verifies_the_exact_entity(self):
        decision = self.decide({"name": "NORDIC SIGNAL AS", "source_url": "https://example.no/"})
        self.assertEqual(decision["status"], EXACT)
        self.assertTrue(decision["publishable"])
        self.assertTrue(decision["signals"]["name_exact"])
        self.assertTrue(decision["signals"]["domain_match"])

    def test_exact_name_domain_and_address_all_match_verifies(self):
        decision = self.decide(
            {"name": "Nordic Signal AS", "source_url": "https://example.no/about", "address": "Karl Johans gate 1, 0159 Oslo"}
        )
        self.assertEqual(decision["status"], EXACT)
        self.assertTrue(decision["signals"]["address_support"]["supported"])

    def test_exact_domain_match_is_supporting_evidence_not_a_verdict(self):
        decision = self.decide({"source_url": "https://shop.example.no/products"})
        self.assertTrue(decision["signals"]["domain_match"])
        self.assertEqual(decision["status"], AMBIGUOUS)
        self.assertFalse(decision["publishable"])

    def test_name_match_alone_never_publishes(self):
        decision = self.decide({"name": "Nordic Signal AS"})
        self.assertEqual(decision["status"], AMBIGUOUS)
        self.assertFalse(decision["publishable"])

    def test_same_name_with_a_different_domain_cannot_establish_identity(self):
        decision = self.decide({"name": "Nordic Signal AS", "source_url": "https://example-other.no/"})
        self.assertEqual(decision["status"], AMBIGUOUS)
        self.assertFalse(decision["publishable"])

    def test_same_name_with_a_different_domain_and_matching_address_cannot_establish_identity(self):
        decision = self.decide(
            {"name": "Nordic Signal AS", "source_url": "https://example-other.no/", "address": "Karl Johans gate 1, 0159 Oslo"}
        )
        self.assertEqual(decision["status"], AMBIGUOUS)
        self.assertFalse(decision["publishable"])

    def test_address_match_alone_never_establishes_exact_identity(self):
        decision = self.decide({"address": "Karl Johans gate 1, 0159 Oslo"})
        self.assertTrue(decision["signals"]["address_support"]["supported"])
        self.assertEqual(decision["status"], UNVERIFIED)
        self.assertFalse(decision["publishable"])

    def test_name_plus_shared_address_is_ambiguous_not_exact(self):
        decision = self.decide({"name": "Nordic Signal AS", "address": "Karl Johans gate 1, 0159 Oslo"})
        self.assertEqual(decision["status"], AMBIGUOUS)
        self.assertFalse(decision["publishable"])

    def test_two_distinct_verified_candidates_resolve_to_ambiguous_and_publish_nothing(self):
        candidates = [
            {"organisation_number": "923609016", "source_url": "https://example.no/"},
            {"name": "Nordic Signal AS", "source_url": "https://example.no/about"},
        ]
        decisions = resolve_candidate_identities(self.target, candidates)
        self.assertEqual([item["status"] for item in decisions], [AMBIGUOUS, AMBIGUOUS])
        self.assertFalse(any(item["publishable"] for item in decisions))

    def test_duplicate_records_of_a_single_identity_stay_exact(self):
        candidates = [
            {"name": "Nordic Signal AS", "source_url": "https://example.no/"},
            {"name": "Nordic Signal AS", "source_url": "https://example.no/about?utm_source=x"},
        ]
        decisions = resolve_candidate_identities(self.target, candidates)
        self.assertEqual([item["status"] for item in decisions], [EXACT, EXACT])

    def test_decision_is_deterministic(self):
        candidate = {"name": "Nordic Signal AS", "source_url": "https://example.no/"}
        self.assertEqual(self.decide(candidate), self.decide(candidate))

    def test_generic_legal_name_with_only_a_shared_address_stays_ambiguous(self):
        target = {
            "organisation_number": "923609016",
            "name": "Eiendom AS",
            "domain": "eiendom-example.no",
            "address": {"street": "Karl Johans gate 1", "postcode": "0159", "city": "Oslo", "municipality": "Oslo"},
        }
        decision = triangulate_identity(target, {"name": "Eiendom AS", "address": "Karl Johans gate 1, 0159 Oslo"})
        self.assertFalse(decision["publishable"])
        self.assertIn(decision["status"], (AMBIGUOUS, UNVERIFIED))


class IdentityTriangulationAdversarialTests(unittest.TestCase):
    """Phase 2: deceptive URLs, lookalike domains and malformed evidence must abstain."""

    def test_lookalike_domain_is_never_the_same_site(self):
        for deceptive in ("example-other.no", "notexample.no", "example.no.evil.com", "myexample.no", "xexample.no"):
            with self.subTest(host=deceptive):
                self.assertFalse(same_site(deceptive, "example.no"))
                decision = triangulate_identity(
                    {"organisation_number": "", "name": "Nordic Signal AS", "domain": "example.no"},
                    {"name": "Nordic Signal AS", "source_url": f"https://{deceptive}/"},
                )
                self.assertFalse(decision["publishable"])

    def test_a_real_subdomain_is_the_same_site(self):
        for host in ("shop.example.no", "www.example.no", "a.b.example.no"):
            with self.subTest(host=host):
                self.assertTrue(same_site(host, "example.no"))

    def test_url_normalization_is_strict_but_host_exact(self):
        for messy in (
            "https://WWW.Example.NO.:8443/a/b?c=1#d",
            "http://www.example.no",
            "example.no.",
            "https://example.no/path",
            "HTTPS://WWW.EXAMPLE.NO",
        ):
            with self.subTest(url=messy):
                self.assertEqual(normalized_domain(messy), "example.no")

    def test_userinfo_and_query_tricks_cannot_impersonate_the_target_domain(self):
        for trick in ("https://example.no@evil.com/", "https://evil.com/?ref=example.no", "https://evil.com/example.no"):
            with self.subTest(url=trick):
                self.assertNotEqual(normalized_domain(trick), "example.no")
                decision = triangulate_identity(
                    {"organisation_number": "", "name": "Nordic Signal AS", "domain": "example.no"},
                    {"name": "Nordic Signal AS", "source_url": trick},
                )
                self.assertFalse(decision["publishable"])

    def test_norwegian_characters_fold_to_the_same_legal_name(self):
        exact, score = name_match("Blåbærsyltetøy AS", "BLABAERSYLTETOY AS")
        self.assertTrue(exact)
        self.assertEqual(score, 1.0)
        decision = triangulate_identity(
            {"organisation_number": "", "name": "Blåbærsyltetøy AS", "domain": "example.no"},
            {"name": "Blåbærsyltetøy AS", "source_url": "https://example.no/"},
        )
        self.assertEqual(decision["status"], EXACT)

    def test_a_near_miss_name_is_not_an_exact_name(self):
        exact, _ = name_match("Nordic Signal AS", "Nordic Signalgruppen AS")
        self.assertFalse(exact)

    def test_formatted_organisation_numbers_abstain_instead_of_guessing(self):
        for declared in ("923 609 016", "923-609-016", "923.609.016", "0923609016"):
            with self.subTest(declared=declared):
                decision = triangulate_identity(
                    {"organisation_number": "923609016", "name": "Nordic Signal AS", "domain": ""},
                    {"organisation_number": declared, "name": "Nordic Signal AS"},
                )
                self.assertFalse(decision["publishable"])
                self.assertEqual(decision["status"], MISMATCHED)

    def test_an_integer_organisation_number_still_matches_exactly(self):
        decision = triangulate_identity(
            {"organisation_number": "923609016", "name": "Nordic Signal AS", "domain": ""},
            {"organisation_number": 923609016, "name": "Someone Else AS"},
        )
        self.assertEqual(decision["status"], EXACT)

    def test_candidate_with_no_identity_signal_is_unverified(self):
        decision = triangulate_identity(
            {"organisation_number": "923609016", "name": "Nordic Signal AS", "domain": "example.no"},
            {"source_url": "https://www.linkedin.com/company/somebody-else"},
        )
        self.assertEqual(decision["status"], UNVERIFIED)
        self.assertFalse(decision["publishable"])

    def test_empty_target_identity_cannot_resolve_a_candidate(self):
        decision = triangulate_identity({}, {"name": "Nordic Signal AS", "source_url": "https://example.no/"})
        self.assertFalse(decision["publishable"])
        self.assertIn(decision["status"], (AMBIGUOUS, UNVERIFIED))

    def test_malformed_candidates_do_not_raise(self):
        for candidate in ({}, {"organisation_number": None}, {"name": None}, {"address": 17}, None, "string"):
            with self.subTest(candidate=candidate):
                decision = triangulate_identity(
                    {"organisation_number": "923609016", "name": "Nordic Signal AS", "domain": "example.no"}, candidate
                )
                self.assertFalse(decision["publishable"])

    def test_a_declared_conflicting_municipality_blocks_an_org_less_candidate(self):
        decision = triangulate_identity(
            {"organisation_number": "", "name": "Nordic Signal AS", "domain": "example.no", "municipality": "Oslo"},
            {"name": "Nordic Signal AS", "municipality": "Bergen"},
        )
        self.assertTrue(decision["signals"]["municipality_conflict"])
        self.assertFalse(decision["publishable"])


class IdentityTargetIdentityExtractionTests(unittest.TestCase):
    """Phase 2: the target identity comes from official records, never from a crawl alone."""

    def profile(self, **overrides):
        profile = {
            "organisation_number": "923609016",
            "name": "Nordic Signal AS",
            "evidence": {
                "registry_live": {
                    "value": {
                        "business_address": {
                            "adresse": "Karl Johans gate 1",
                            "postnummer": "0159",
                            "poststed": "Oslo",
                            "kommune": "Oslo",
                        },
                        "website": "https://www.example.no/",
                    }
                },
            },
        }
        profile.update(overrides)
        return profile

    def test_target_identity_uses_the_official_registered_website(self):
        identity = canonical_identity_from_profile(self.profile())
        self.assertEqual(identity["organisation_number"], "923609016")
        self.assertEqual(identity["name"], "Nordic Signal AS")
        self.assertEqual(identity["domain"], "example.no")
        self.assertEqual(identity["address"]["postcode"], "0159")
        self.assertEqual(identity["municipality"], "Oslo")

    def test_a_quarantined_website_can_never_become_an_identity_anchor(self):
        profile = self.profile()
        profile["evidence"]["registry_live"]["value"]["website"] = ""
        profile["evidence"]["website"] = {
            "status": "available",
            "value": {
                "final_url": "https://impostor-example.net/",
                "identity_assessment": {"status": "related_or_uncertain", "publishable": False},
            },
        }
        self.assertEqual(canonical_identity_from_profile(profile)["domain"], "")

    def test_a_publishable_website_supplies_the_domain_when_the_registry_has_none(self):
        profile = self.profile()
        profile["evidence"]["registry_live"]["value"]["website"] = ""
        profile["evidence"]["website"] = {
            "status": "available",
            "value": {
                "final_url": "https://www.example.no/about",
                "identity_assessment": {"status": "exact", "publishable": True},
            },
        }
        self.assertEqual(canonical_identity_from_profile(profile)["domain"], "example.no")

    def test_flattened_address_keys_are_understood(self):
        target = canonical_identity_from_task(
            {
                "organisation_number": "923609016",
                "company_name": "Nordic Signal AS",
                "target_identity": {"address": {"forretningsadresse.adresse": "Karl Johans gate 1", "forretningsadresse.postnummer": "0159"}},
            }
        )
        support = address_support(target["address"], "Karl Johans gate 1, 0159 Oslo")
        self.assertTrue(support["supported"])

    def test_a_task_without_target_identity_falls_back_to_its_own_fields(self):
        target = canonical_identity_from_task(
            {"organisation_number": "923609016", "company_name": "Nordic Signal AS", "municipality": "OSLO"}
        )
        self.assertEqual(target["organisation_number"], "923609016")
        self.assertEqual(target["name"], "Nordic Signal AS")
        self.assertEqual(target["domain"], "")
        decision = triangulate_identity(target, {"organisation_number": "923609016", "name": "Someone Else AS"})
        self.assertEqual(decision["status"], EXACT)

    def test_the_planner_carries_the_target_identity_into_every_task(self):
        tasks = plan_external_tasks(self.profile())
        self.assertTrue(tasks)
        for task in tasks:
            self.assertEqual(task["target_identity"]["organisation_number"], "923609016")
            self.assertEqual(task["target_identity"]["domain"], "example.no")
            self.assertEqual(task["target_identity"]["address"]["postcode"], "0159")


class _ClaimingConnector(BaseConnector):
    """A connector that asserts exact identity without proving it."""

    name = "claim_connector"
    acquisition_mode = "official_api"
    rights_status = "approved"
    supported_task_types = {"resolve_places_and_public_rating"}
    cost_per_request = 0.0

    def __init__(self, observation):
        self.observation = observation

    def estimate_requests(self, task):
        return 1

    def execute(self, task, budget, *, now):
        return ConnectorResult.success(task, [dict(self.observation)], requests_used=1, cost=0.0)


class ExternalExecutorIdentityGateTests(unittest.TestCase):
    """Phase 2: a connector cannot assert its way past the identity gate."""

    now = "2026-01-01T00:00:00Z"

    def observation(self, **overrides):
        observation = {
            "id": "claim-0001-0000000",
            "exact_entity": True,
            "identity_proof": [{"type": "domain_match", "value": "example.no"}],
            "platform": "google_places",
            "signal_type": "place_summary",
            "source_url": "https://example.no/places/1",
            "content_sha256": "a" * 64,
            "acquisition_mode": "official_api",
            "rights_status": "approved",
        }
        observation.update(overrides)
        return observation

    def run_claim(self, observation):
        task = mock_task(
            connector="claim_connector",
            company_name="Example AS",
            extra={
                "target_identity": {
                    "organisation_number": "923609016",
                    "name": "Example AS",
                    "domain": "example.no",
                    "address": {"street": "Karl Johans gate 1", "postcode": "0159", "city": "Oslo", "municipality": "OSLO"},
                }
            },
        )
        registry = ConnectorRegistry([_ClaimingConnector(observation)])
        return run_external_tasks([task], registry=registry, budget=RequestBudget(20), now=self.now)

    def test_a_connector_assertion_without_independent_evidence_is_rejected(self):
        result = self.run_claim(self.observation())
        self.assertEqual(result["observations"], [])
        self.assertEqual(len(result["rejected_observations"]), 1)
        reasons = result["rejected_observations"][0]["reasons"]
        self.assertIn("identity triangulation resolved the candidate as ambiguous", reasons)

    def test_name_and_domain_evidence_lets_the_observation_through(self):
        result = self.run_claim(self.observation(name="Example AS", source_url="https://www.example.no/about"))
        self.assertEqual(result["rejected_observations"], [])
        self.assertEqual(len(result["observations"]), 1)
        self.assertEqual(validate_observation(result["observations"][0]), [])

    def test_a_lookalike_domain_is_rejected_even_with_an_exact_name(self):
        result = self.run_claim(self.observation(name="Example AS", source_url="https://example-other.no/"))
        self.assertEqual(result["observations"], [])
        self.assertIn("identity triangulation resolved the candidate as ambiguous", result["rejected_observations"][0]["reasons"])

    def test_a_foreign_organisation_number_is_rejected_even_with_a_perfect_domain(self):
        result = self.run_claim(self.observation(organisation_number="111222333", name="Example AS"))
        self.assertEqual(result["observations"], [])
        self.assertEqual(result["rejected_observations"][0]["reasons"], ["organisation number does not match the planned task"])

    def test_identity_failure_never_stamps_the_planned_organisation_number_onto_the_candidate(self):
        result = self.run_claim(self.observation(name="Example AS", source_url="https://example-other.no/"))
        self.assertNotIn("https://example-other.no", json.dumps(result["rejected_observations"], ensure_ascii=False))


class ObservationEvidenceClaimTests(unittest.TestCase):
    """Phase 3: the single deterministic observation -> evidence -> claim path."""

    now = "2026-01-01T00:00:00Z"

    def profile(self, **overrides):
        profile = {"organisation_number": "923609016", "name": "Example AS", "evidence": {}}
        profile.update(overrides)
        return profile

    def observation(self, **overrides):
        observation = {
            "id": "obs-0001-0000000",
            "task_id": "task-0001-0000000",
            "connector": "google_places_api",
            "organisation_number": "923609016",
            "platform": "google_places",
            "signal_type": "place_summary",
            "source_url": "https://example.no/places/1",
            "retrieved_at": "2026-01-01T00:00:00Z",
            "content_sha256": "a" * 64,
            "exact_entity": True,
            "identity_proof": [{"type": "domain_match", "value": "example.no"}],
            "acquisition_mode": "official_api",
            "rights_status": "approved",
            "metrics": {"rating": 4.5, "review_count": 87},
        }
        observation.update(overrides)
        return observation

    def test_a_valid_observation_becomes_evidence_and_published_claims(self):
        built = build_evidence_and_claims(self.profile(), [self.observation()])
        self.assertEqual(built["rejections"], [])
        self.assertEqual(len(built["evidence"]), 1)
        record = built["evidence"][0]
        self.assertEqual(record["status"], "available")
        self.assertEqual(record["organisation_number"], "923609016")
        self.assertEqual(record["identity"]["status"], "exact")
        self.assertEqual(record["identity"]["organisation_number"], "923609016")
        self.assertEqual(record["source_url"], "https://example.no/places/1")
        self.assertEqual(record["content_sha256"], "a" * 64)
        self.assertEqual(record["retrieved_at"], "2026-01-01T00:00:00Z")
        self.assertEqual(record["acquisition_mode"], "official_api")
        self.assertEqual(record["rights_status"], "approved")
        self.assertEqual(record["evidence_id"], evidence_id("923609016", "https://example.no/places/1", "a" * 64, record["value"]))
        self.assertEqual(record["unsupported_fact_fields"], [])
        self.assertEqual({item["field"] for item in record["value"]}, {"rating", "review_count"})

        claims = built["claims"]
        self.assertEqual({item["field"] for item in claims}, {"rating", "review_count"})
        for claim in claims:
            self.assertEqual(claim["state"], CLAIM_PUBLISHED)
            self.assertEqual(claim["classification"], CLAIM_CLASSIFICATION)
            self.assertEqual(claim["organisation_number"], "923609016")
            self.assertEqual(claim["evidence_ids"], [record["evidence_id"]])
            self.assertEqual(claim["source_urls"], ["https://example.no/places/1"])
            self.assertEqual(claim["method"], METHOD)
            self.assertEqual(claim["identity"]["status"], "exact")
            self.assertEqual(
                claim["claim_id"],
                claim_id("923609016", claim["field"], claim["normalized_value"], claim["state"]),
            )
            self.assertNotIn("confidence", claim)
            self.assertNotIn("score", claim)
            self.assertNotIn("probability", claim)
        by_field = {item["field"]: item for item in claims}
        self.assertEqual(by_field["rating"]["value"], 4.5)
        self.assertEqual(by_field["review_count"]["value"], 87)

    def test_published_claims_are_carried_by_the_terminal_envelope(self):
        built = build_evidence_and_claims(self.profile(), [self.observation()])
        profile = self.profile(external_claims=built["claims"])
        envelope = terminal_envelope(profile, run_id="run-1", modules=[], started_at=self.now, completed_at=self.now)
        required = {
            "run_id",
            "organisation_number",
            "state",
            "started_at",
            "completed_at",
            "modules",
            "profile",
            "claims",
        }
        self.assertTrue(required.issubset(set(envelope)))
        self.assertEqual(envelope["claims"], built["claims"])
        self.assertEqual(envelope["state"], "complete")
        plain = terminal_envelope(self.profile(), run_id="run-1", modules=[], started_at=self.now, completed_at=self.now)
        self.assertEqual(plain["claims"], [])

    def test_a_wrong_company_observation_never_produces_evidence_or_a_claim(self):
        built = build_evidence_and_claims(self.profile(), [self.observation(organisation_number="111222333")])
        self.assertEqual(built["evidence"], [])
        self.assertEqual(built["claims"], [])
        self.assertEqual(len(built["rejections"]), 1)
        rejection = built["rejections"][0]
        self.assertEqual(rejection["state"], "mismatched")
        self.assertEqual(rejection["organisation_number"], "923609016")
        for leaked in ("value", "source_url", "content_sha256", "identity_proof", "evidence_id", "metrics"):
            self.assertNotIn(leaked, rejection)
        self.assertNotIn("4.5", json.dumps(built, ensure_ascii=False))

    def test_an_ambiguous_identity_never_produces_a_claim(self):
        observation = self.observation(name="Example AS")
        observation.pop("organisation_number")
        built = build_evidence_and_claims(self.profile(), [observation])
        self.assertEqual(built["evidence"], [])
        self.assertEqual(built["claims"], [])
        self.assertEqual(built["rejections"][0]["state"], "ambiguous")

    def test_an_unverified_identity_never_produces_a_claim(self):
        observation = self.observation()
        observation.pop("organisation_number")
        built = build_evidence_and_claims(self.profile(), [observation])
        self.assertEqual(built["evidence"], [])
        self.assertEqual(built["claims"], [])
        self.assertEqual(built["rejections"][0]["state"], "unverified")

    def test_unapproved_acquisition_mode_blocks_any_claim(self):
        built = build_evidence_and_claims(self.profile(), [self.observation(acquisition_mode="jobspy_experiment")])
        self.assertEqual(built["evidence"], [])
        self.assertEqual(built["claims"], [])
        rejection = built["rejections"][0]
        self.assertEqual(rejection["state"], "blocked")
        self.assertIn("acquisition mode is not approved for publication", rejection["reasons"])

    def test_unapproved_rights_block_any_claim(self):
        built = build_evidence_and_claims(self.profile(), [self.observation(rights_status="review_required")])
        self.assertEqual(built["evidence"], [])
        self.assertEqual(built["claims"], [])
        rejection = built["rejections"][0]
        self.assertEqual(rejection["state"], "blocked")
        self.assertIn("source rights are not approved", rejection["reasons"])

    def test_identity_failures_outrank_rights_blocks(self):
        wrong_company = self.observation(organisation_number="111222333", acquisition_mode="jobspy_experiment")
        self.assertEqual(build_evidence_and_claims(self.profile(), [wrong_company])["rejections"][0]["state"], "mismatched")
        observation = self.observation(name="Example AS", rights_status="review_required")
        observation.pop("organisation_number")
        self.assertEqual(build_evidence_and_claims(self.profile(), [observation])["rejections"][0]["state"], "ambiguous")

    def test_a_profile_without_an_organisation_number_never_receives_a_claim(self):
        profile = self.profile(
            organisation_number="",
            evidence={"registry_live": {"value": {"website": "https://example.no"}}},
        )
        built = build_evidence_and_claims(profile, [self.observation(name="Example AS")])
        self.assertEqual(built["evidence"], [])
        self.assertEqual(built["claims"], [])
        self.assertEqual(built["rejections"][0]["state"], "unverified")

    def test_the_same_observation_twice_never_duplicates_a_claim(self):
        observation = self.observation()
        built = build_evidence_and_claims(self.profile(), [observation, observation])
        self.assertEqual(len(built["evidence"]), 1)
        self.assertEqual(len(built["claims"]), 2)
        for claim in built["claims"]:
            self.assertEqual(len(claim["evidence_ids"]), 1)
        self.assertEqual(built["rejections"], [])

    def test_two_sources_corroborate_one_claim(self):
        second = self.observation(
            id="obs-0002-0000000",
            source_url="https://example.no/places/2",
            content_sha256="b" * 64,
        )
        built = build_evidence_and_claims(self.profile(), [self.observation(), second])
        self.assertEqual(len(built["evidence"]), 2)
        self.assertEqual(len(built["claims"]), 2)
        for claim in built["claims"]:
            self.assertEqual(len(claim["evidence_ids"]), 2)
            self.assertEqual(claim["source_urls"], ["https://example.no/places/1", "https://example.no/places/2"])
            self.assertEqual(claim["state"], CLAIM_PUBLISHED)

    def test_every_observation_ends_as_evidence_or_as_an_explicit_rejection(self):
        observations = [
            self.observation(),
            self.observation(id="obs-0002-0000000", organisation_number="111222333"),
            self.observation(id="obs-0003-0000000", rights_status="review_required"),
            self.observation(id="obs-0004-0000000", name="Example AS", organisation_number=None),
        ]
        built = build_evidence_and_claims(self.profile(), observations)
        self.assertEqual(len(built["evidence"]) + len(built["rejections"]), len(observations))
        self.assertEqual({item["state"] for item in built["rejections"]}, {"mismatched", "blocked", "ambiguous"})

    def test_identical_input_produces_identical_output_and_no_reported_change(self):
        first = build_evidence_and_claims(self.profile(), [self.observation()])
        second = build_evidence_and_claims(self.profile(), [self.observation()])
        self.assertEqual(json.dumps(first, ensure_ascii=False, sort_keys=True), json.dumps(second, ensure_ascii=False, sort_keys=True))
        self.assertEqual(diff_claims(first["claims"], second["claims"]), [])

    def test_a_retrieval_timestamp_change_alone_is_not_a_claim_change(self):
        first = build_evidence_and_claims(self.profile(), [self.observation()])
        refetched = build_evidence_and_claims(
            self.profile(), [self.observation(retrieved_at="2026-02-01T00:00:00Z")]
        )
        self.assertEqual(first["evidence"][0]["evidence_id"], refetched["evidence"][0]["evidence_id"])
        self.assertEqual(diff_claims(first["claims"], refetched["claims"]), [])

    def test_changed_source_content_is_a_real_claim_change(self):
        first = build_evidence_and_claims(self.profile(), [self.observation()])
        changed = build_evidence_and_claims(self.profile(), [self.observation(content_sha256="b" * 64)])
        changes = diff_claims(first["claims"], changed["claims"])
        self.assertEqual(len(changes), 2)
        self.assertEqual({item["change"] for item in changes}, {"changed"})
        for item in changes:
            self.assertNotEqual(item["evidence_ids"], [])
            self.assertNotEqual(item["old_value"], None)

    def test_claims_that_disappear_are_reported_as_removed_and_new_ones_as_added(self):
        built = build_evidence_and_claims(self.profile(), [self.observation()])
        changes = diff_claims([], built["claims"])
        self.assertEqual({item["change"] for item in changes}, {"added"})
        self.assertEqual({item["change"] for item in diff_claims(built["claims"], [])}, {"removed"})

    def test_conflicting_values_produce_one_explicit_conflict_claim(self):
        first = self.observation(metrics={"rating": 4.5})
        second = self.observation(
            id="obs-0002-0000000",
            source_url="https://example.no/places/2",
            content_sha256="b" * 64,
            metrics={"rating": 3.0},
        )
        built = build_evidence_and_claims(self.profile(), [first, second])
        self.assertEqual(len(built["claims"]), 1)
        claim = built["claims"][0]
        self.assertEqual(claim["state"], CLAIM_CONFLICT)
        self.assertEqual(claim["normalized_value"], [3.0, 4.5])
        self.assertIn(claim["value"], (3.0, 4.5))
        self.assertEqual(len(claim["evidence_ids"]), 2)
        self.assertEqual(len(claim["source_urls"]), 2)

    def test_conflict_handling_never_depends_on_input_order(self):
        first = self.observation(metrics={"rating": 4.5})
        second = self.observation(
            id="obs-0002-0000000",
            source_url="https://example.no/places/2",
            content_sha256="b" * 64,
            metrics={"rating": 3.0, "review_count": 12},
        )
        forward = build_evidence_and_claims(self.profile(), [first, second])
        reverse = build_evidence_and_claims(self.profile(), [second, first])
        self.assertEqual(
            json.dumps(forward["claims"], ensure_ascii=False, sort_keys=True),
            json.dumps(reverse["claims"], ensure_ascii=False, sort_keys=True),
        )
        self.assertEqual(
            json.dumps(forward["evidence"], ensure_ascii=False, sort_keys=True),
            json.dumps(reverse["evidence"], ensure_ascii=False, sort_keys=True),
        )

    def test_profiles_never_receive_each_others_claims(self):
        other = self.profile(organisation_number="111222333", name="Other AS")
        leaked = build_evidence_and_claims(other, [self.observation()])
        self.assertEqual(leaked["claims"], [])
        self.assertEqual(leaked["rejections"][0]["state"], "mismatched")
        own = build_evidence_and_claims(other, [self.observation(organisation_number="111222333")])
        self.assertEqual(len(own["claims"]), 2)
        self.assertEqual({item["organisation_number"] for item in own["claims"]}, {"111222333"})
        mine = build_evidence_and_claims(self.profile(), [self.observation()])
        self.assertFalse({item["claim_id"] for item in own["claims"]} & {item["claim_id"] for item in mine["claims"]})

    def test_two_distinct_exact_candidates_in_one_batch_are_all_downgraded(self):
        profile = self.profile(evidence={"registry_live": {"value": {"website": "https://example.no"}}})
        first = self.observation()
        second = self.observation(
            id="obs-0002-0000000",
            organisation_number=None,
            name="Example AS",
            source_url="https://example.no/about",
        )
        built = build_evidence_and_claims(profile, [first, second])
        self.assertEqual(built["evidence"], [])
        self.assertEqual(built["claims"], [])
        self.assertEqual(len(built["rejections"]), 2)
        self.assertEqual({item["state"] for item in built["rejections"]}, {"ambiguous"})

    def test_unserializable_fact_values_are_reported_and_never_silently_dropped(self):
        observation = self.observation(metrics={"rating": 4.5, "headcount": b"bytes"})
        facts, unsupported = structured_facts(observation)
        self.assertEqual(unsupported, ["headcount"])
        self.assertEqual([item[0] for item in facts], ["rating"])
        built = build_evidence_and_claims(self.profile(), [observation])
        self.assertEqual(built["evidence"][0]["unsupported_fact_fields"], ["headcount"])
        self.assertEqual({item["field"] for item in built["claims"]}, {"rating"})

    def test_facts_use_the_canonical_connector_contract(self):
        observation = self.observation(facts=[{"field": "employee_count", "value": 12}])
        built = build_evidence_and_claims(self.profile(), [observation])
        self.assertEqual(len(built["claims"]), 1)
        self.assertEqual(built["claims"][0]["value"], 12)
        self.assertEqual(built["claims"][0]["normalized_value"], 12.0)

    def test_claim_value_normalization_collapses_equivalent_representations(self):
        self.assertEqual(normalize_claim_value("  Big   Corp "), "big corp")
        self.assertEqual(normalize_claim_value(87), 87.0)
        self.assertEqual(normalize_claim_value(87.0), 87.0)
        self.assertEqual(normalize_claim_value(True), True)
        self.assertEqual(normalize_claim_value({"b": 1, "a": 2}), '{"a":2,"b":1}')
        self.assertEqual(normalize_claim_value("BIG CORP"), normalize_claim_value("big corp"))

    def test_equivalent_values_from_two_sources_never_become_a_conflict(self):
        first = self.observation(metrics={"review_count": 87})
        second = self.observation(
            id="obs-0002-0000000",
            source_url="https://example.no/places/2",
            content_sha256="b" * 64,
            metrics={"review_count": 87.0},
        )
        built = build_evidence_and_claims(self.profile(), [first, second])
        self.assertEqual(len(built["claims"]), 1)
        claim = built["claims"][0]
        self.assertEqual(claim["state"], CLAIM_PUBLISHED)
        self.assertEqual(claim["normalized_value"], 87.0)
        self.assertEqual(len(claim["evidence_ids"]), 2)

    def test_executor_accepted_observations_flow_into_the_claim_pipeline(self):
        observation = self.observation(
            name="Example AS",
            source_url="https://www.example.no/about",
            metrics={"rating": 4.5, "review_count": 87},
        )
        observation.pop("organisation_number", None)
        task = mock_task(
            connector="claim_connector",
            company_name="Example AS",
            extra={
                "target_identity": {
                    "organisation_number": "923609016",
                    "name": "Example AS",
                    "domain": "example.no",
                    "address": {"street": "Karl Johans gate 1", "postcode": "0159", "city": "Oslo", "municipality": "OSLO"},
                }
            },
        )
        result = run_external_tasks(
            [task],
            registry=ConnectorRegistry([_ClaimingConnector(observation)]),
            budget=RequestBudget(20),
            now=self.now,
        )
        self.assertEqual(result["rejected_observations"], [])
        self.assertEqual(len(result["observations"]), 1)
        built = build_evidence_and_claims(self.profile(), result["observations"])
        self.assertEqual(built["rejections"], [])
        self.assertEqual({item["field"] for item in built["claims"]}, {"rating", "review_count"})
        for claim in built["claims"]:
            self.assertEqual(claim["state"], CLAIM_PUBLISHED)
            self.assertEqual(claim["organisation_number"], "923609016")
            self.assertEqual(claim["source_url"], "https://www.example.no/about")

    def test_blocked_or_unverified_evidence_never_becomes_a_claim(self):
        built = build_evidence_and_claims(self.profile(), [self.observation()])
        record = built["evidence"][0]
        self.assertEqual(len(assemble_claims([record], "923609016")), 2)
        blocked = {**record, "status": "blocked"}
        unverified = {**record, "identity": {**record["identity"], "status": "ambiguous"}}
        mismatched = {**record, "identity": {**record["identity"], "status": "mismatched"}}
        self.assertEqual(assemble_claims([blocked, unverified, mismatched], "923609016"), [])

    def test_envelopes_with_claims_still_validate_as_terminal(self):
        built = build_evidence_and_claims(self.profile(), [self.observation()])
        profile = self.profile(external_claims=built["claims"])
        envelopes = [
            terminal_envelope(profile, run_id="run-1", modules=["registry"], started_at=self.now, completed_at=self.now)
        ]
        validation = validate_envelopes(envelopes, 1)
        self.assertTrue(validation["passed"], validation)
        self.assertEqual(validation["invalid_states"], [])


if __name__ == "__main__":
    unittest.main()
