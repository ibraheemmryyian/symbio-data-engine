#!/usr/bin/env python3
"""
enrich_targeted.py
===================
Targeted enrichment: 150 investors + 850 best clients = 1000 credits total.

Investor priority order:
  1. inv_cvc_industrial  (corporate VCs — understand the space, can be customers)
  2. inv_vc_partners     (GPs with industrial/climate portfolio)
  3. inv_pe              (PE — check size matters for Series A+)
  4. inv_family_office   (patient capital, MENA family offices especially)
  5. inv_vc_principals   (fill remaining slots)

Client priority order:
  1. sellers_mena        (home market, regulatory pressure, your network)
  2. sellers_sustainability (CSOs/HSE — own the waste problem)
  3. buyers_procurement  (CPOs — pay for feedstock today)
  4. sellers_ops_chemicals (plant managers — chemicals is highest-value waste)
  5. sellers_ops_metals  (steel slag, metal dust — huge volumes)

Usage:
  python enrich_targeted.py --dry-run   # show who will be enriched, no credits spent
  python enrich_targeted.py             # run for real
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent))
from amplemarket.client import AmplemarketClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

API_KEY = os.getenv("AMPLEMARKET_API_KEY")
if not API_KEY:
    sys.exit("ERROR: AMPLEMARKET_API_KEY not set in .env")

OUT_DIR = Path("exports/amplemarket_drain")
BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M")

INVESTOR_BUDGET = 150
CLIENT_BUDGET = 850

# Priority order within each bucket
INVESTOR_WAVES = [
    "inv_cvc_industrial",
    "inv_vc_partners",
    "inv_pe",
    "inv_family_office",
    "inv_vc_principals",
    "inv_cvc_tech",
    "inv_angels",
    "inv_accelerators",
]

CLIENT_WAVES = [
    "sellers_mena",
    "sellers_sustainability",
    "buyers_procurement",
    "sellers_ops_chemicals",
    "sellers_ops_metals",
    "buyers_procurement_mena",
    "buyers_ceo_industrial",
    "sellers_ops_manufacturing",
    "sellers_ops_food",
]


def load_wave(wave_name: str) -> list[dict]:
    path = OUT_DIR / f"wave_{wave_name}.json"
    if not path.exists():
        log.warning(f"  Wave file not found: {path.name} (run --phase search first)")
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def pick_best(contacts: list[dict], budget: int, contact_type: str = "prospect") -> list[dict]:
    """
    Pick the best contacts within budget.
    Sort: C-suite first, then VP, then Director, then Manager.
    Within same seniority: MENA first (higher intent for this product).
    Filters out irrelevant industries (universities, hospitals, retail, etc.)
    """
    MENA = {"United Arab Emirates", "Saudi Arabia", "Kuwait", "Qatar",
            "Bahrain", "Oman", "Egypt", "Morocco", "Jordan"}

    SENIORITY_RANK = {"c_suite": 0, "vp": 1, "director": 2, "manager": 3,
                      "individual": 4, "unknown": 5}

    # Titles that disqualify any contact regardless of wave
    JUNK_TITLE_TERMS = [
        "instructor", "lecturer", "professor", "teacher", "tutor", "faculty",
        "physician", "doctor", "surgeon", "dentist", "nurse", "therapist",
        "pharmacist", "radiologist", "cardiologist", "pediatrician",
        "real estate agent", "broker", "realtor",
        "pastor", "priest", "imam", "chaplain",
        "fitness coach", "personal trainer",
        "hair stylist", "beautician",
        "chef", "cook", "sous chef",
        "flight attendant", "cabin crew",
        "social worker", "counselor",
    ]

    # For investors: title MUST contain at least one of these signals
    INVESTOR_TITLE_SIGNALS = [
        "partner", "investor", "venture", "investment", "portfolio",
        "chief strategy", "head of innovation", "corporate development",
        "open innovation", "new ventures", "accelerator", "incubator",
        "cio", "chief investment", "cfo", "chief financial",
        "fund", "capital", "equity", "asset management",
        "managing director", "general partner", "principal",
        "angel", "board member", "board director", "non-executive director",
        "ecosystem", "startup", "entrepreneurship",
        "head of corporate", "vp corporate", "director of strategy",
    ]

    # For clients: title MUST contain at least one of these signals
    CLIENT_TITLE_SIGNALS = [
        "cso", "chief sustainability", "head of sustainability", "sustainability",
        "esg", "environment", "hse", "ehs", "circular economy", "waste",
        "ceo", "chief executive", "managing director", "general manager",
        "coo", "chief operating", "operations",
        "plant manager", "site manager", "facility manager", "production manager",
        "process manager", "manufacturing",
        "cpo", "chief procurement", "procurement", "supply chain", "sourcing",
        "raw material", "category manager",
        "vp", "vice president", "director",
    ]

    def is_relevant(c: dict) -> bool:
        title = (c.get("title") or "").lower()

        # Discard junk titles regardless of wave
        for junk in JUNK_TITLE_TERMS:
            if junk in title:
                return False

        if contact_type == "investor":
            return any(sig in title for sig in INVESTOR_TITLE_SIGNALS)
        else:
            return any(sig in title for sig in CLIENT_TITLE_SIGNALS)

    # Only pick contacts with LinkedIn URL and no email yet
    eligible = [c for c in contacts
                if c.get("linkedin_url") and not c.get("email") and is_relevant(c)]

    def sort_key(c):
        sen = SENIORITY_RANK.get(c.get("seniority", "unknown"), 5)
        is_mena = 0 if c.get("country", "") in MENA else 1
        return (sen, is_mena, c.get("company", ""))

    eligible.sort(key=sort_key)
    return eligible[:budget]


def run_enrichment(client: AmplemarketClient, contacts: list[dict], label: str, dry_run: bool) -> list[dict]:
    if not contacts:
        log.info(f"  No contacts to enrich for {label}")
        return []

    log.info(f"\n=== ENRICH: {label} ({len(contacts)} contacts) ===")

    # Show breakdown
    from collections import Counter
    sen_counts = Counter(c.get("seniority", "unknown") for c in contacts)
    country_counts = Counter(c.get("country", "unknown") for c in contacts)
    wave_counts = Counter(c.get("wave", "unknown") for c in contacts)

    log.info(f"  Seniority: {dict(sen_counts.most_common(5))}")
    log.info(f"  Top countries: {dict(country_counts.most_common(8))}")
    log.info(f"  By wave: {dict(wave_counts.most_common())}")

    if dry_run:
        log.info(f"  [DRY RUN] Would spend {len(contacts)} credits. Sample:")
        for c in contacts[:10]:
            log.info(f"    {c.get('full_name','?'):30} | {c.get('title','?')[:40]:40} | {c.get('company','?')[:30]:30} | {c.get('country','?')}")
        return []

    leads = [{"linkedin_url": c["linkedin_url"]} for c in contacts]
    li_to_contact = {c["linkedin_url"]: c for c in contacts}

    try:
        request_id = client.submit_people_enrichment(leads)
        log.info(f"  Request ID: {request_id} — polling...")
    except Exception as e:
        log.error(f"  Submission failed: {e}")
        return []

    enriched = []
    try:
        for page in client.poll_people_enrichment(request_id, timeout=7200):
            for person in page:
                # Each item is a wrapper: {"linkedin_url":..., "status":"enriched", "result":{...person data...}}
                # Real email/phone/company fields live inside "result", not the wrapper
                inner_person = person.get("result", person)
                li = inner_person.get("linkedin_url") or person.get("linkedin_url")
                if li and li in li_to_contact:
                    orig = li_to_contact[li]
                    # Merge enriched data back from inner person object
                    merged = dict(orig)
                    merged["email"] = inner_person.get("email") or orig.get("email")
                    merged["email_verified"] = inner_person.get("email_verified", False)
                    merged["phone"] = inner_person.get("phone") or inner_person.get("mobile_phone") or orig.get("phone")
                    merged["company_domain"] = inner_person.get("company_domain") or inner_person.get("website") or orig.get("company_domain")
                    merged["company_size"] = inner_person.get("company_size") or inner_person.get("headcount") or orig.get("company_size")
                    enriched.append(merged)

            # Save checkpoint
            chk = OUT_DIR / f"enriched_{label.replace(' ', '_')}_{BATCH_ID}.json"
            with open(chk, "w", encoding="utf-8") as f:
                json.dump(enriched, f, indent=2, ensure_ascii=False)
            log.info(f"  Checkpoint: {len(enriched)} enriched so far -> {chk.name}")
    except Exception as e:
        log.error(f"  Polling error: {e}")

    log.info(f"  Done: {len(enriched)} emails retrieved")
    return enriched


def export_results(investors_enriched: list[dict], clients_enriched: list[dict]):
    all_enriched = investors_enriched + clients_enriched

    FIELDS = [
        # Scoring / meta
        "priority", "contact_type", "prospect_side", "seniority",
        # Person identity
        "full_name", "first_name", "last_name", "gender",
        "title", "headline",
        # Contact
        "email", "email_verified", "phone", "linkedin_url",
        # Person location
        "country", "city", "state",
        # Role history
        "current_role_start", "prev_companies",
        # Company basics
        "company", "company_linkedin_url", "company_website",
        "company_industry", "company_size", "company_headcount", "company_revenue",
        # Company location
        "company_country", "company_city",
        # Company profile
        "company_type", "company_founded", "company_is_b2b",
        # Funding
        "total_funding", "funding_stage", "funding_date", "latest_funding_amount",
        # Tech & keywords
        "technologies", "company_keywords",
        # Batch metadata
        "wave", "wave_label", "source",
    ]

    def write_csv(rows, path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        log.info(f"  Wrote {len(rows)} rows -> {path}")

    csv_all = OUT_DIR / f"enriched_all_{BATCH_ID}.csv"
    csv_inv = OUT_DIR / f"enriched_investors_{BATCH_ID}.csv"
    csv_cli = OUT_DIR / f"enriched_clients_{BATCH_ID}.csv"

    write_csv(all_enriched, csv_all)
    if investors_enriched:
        write_csv(investors_enriched, csv_inv)
    if clients_enriched:
        write_csv(clients_enriched, csv_cli)

    with_email = [c for c in all_enriched if c.get("email")]
    log.info(f"\n=== ENRICHMENT SUMMARY ===")
    log.info(f"  Investors enriched : {len(investors_enriched)}")
    log.info(f"  Clients enriched   : {len(clients_enriched)}")
    log.info(f"  Total with email   : {len(with_email)}")
    log.info(f"  Files: {csv_all.name}, {csv_inv.name}, {csv_cli.name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Show who would be enriched without spending credits")
    args = parser.parse_args()

    if args.dry_run:
        log.info("=== DRY RUN — no credits will be spent ===")

    client = AmplemarketClient(API_KEY)

    # ---- Build investor pool ----
    log.info(f"\nBuilding investor pool (budget: {INVESTOR_BUDGET})...")
    investor_pool = []
    seen_inv = set()
    for wave_name in INVESTOR_WAVES:
        contacts = load_wave(wave_name)
        for c in contacts:
            key = c.get("linkedin_url") or f"{c.get('full_name')}_{c.get('company')}"
            if key not in seen_inv:
                seen_inv.add(key)
                investor_pool.append(c)

    investors_to_enrich = pick_best(investor_pool, INVESTOR_BUDGET, contact_type="investor")
    log.info(f"  Investor pool: {len(investor_pool)} contacts -> selected top {len(investors_to_enrich)}")

    # ---- Build client pool ----
    log.info(f"\nBuilding client pool (budget: {CLIENT_BUDGET})...")
    client_pool = []
    seen_cli = set()
    for wave_name in CLIENT_WAVES:
        contacts = load_wave(wave_name)
        for c in contacts:
            key = c.get("linkedin_url") or f"{c.get('full_name')}_{c.get('company')}"
            if key not in seen_cli:
                seen_cli.add(key)
                client_pool.append(c)

    clients_to_enrich = pick_best(client_pool, CLIENT_BUDGET, contact_type="prospect")
    log.info(f"  Client pool: {len(client_pool)} contacts -> selected top {len(clients_to_enrich)}")

    log.info(f"\nTotal credits to spend: {len(investors_to_enrich)} + {len(clients_to_enrich)} = {len(investors_to_enrich) + len(clients_to_enrich)}")

    # ---- Run enrichment ----
    investors_enriched = run_enrichment(client, investors_to_enrich, "investors", args.dry_run)
    clients_enriched = run_enrichment(client, clients_to_enrich, "clients", args.dry_run)

    if not args.dry_run:
        export_results(investors_enriched, clients_enriched)


if __name__ == "__main__":
    main()
