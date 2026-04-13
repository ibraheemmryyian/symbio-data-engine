"""
Amplemarket Automated Pipeline
================================
Full automation: search → enrich → store in local DB.

Usage:
    python -m amplemarket.pipeline --mode investors
    python -m amplemarket.pipeline --mode procurement
    python -m amplemarket.pipeline --mode eprtr         # your moat hack
    python -m amplemarket.pipeline --mode all
    python -m amplemarket.pipeline --credits            # check remaining credits
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from amplemarket.client import AmplemarketClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ============================================
# Search configurations — each wave burns credits
# deliberately and targets a specific ICP
# ============================================

SEARCH_WAVES = {

    # ---- INVESTORS ----

    "investors_vc_partners": {
        "label": "VC Partners — any sector",
        "contact_type": "investor", "investor_type": "vc",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["General Partner", "Managing Partner", "Partner", "Venture Partner", "Investment Partner"]},
    },
    "investors_vc_principals": {
        "label": "VC Principals & Directors",
        "contact_type": "investor", "investor_type": "vc",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Principal", "Investment Director", "Associate", "Senior Associate", "Vice President"]},
    },
    "investors_pe": {
        "label": "Private Equity",
        "contact_type": "investor", "investor_type": "pe",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Managing Director", "Partner", "Principal", "Director", "Investment Manager"],
                    "industries": ["Private Equity", "Investment Management", "Venture Capital & Private Equity"]},
    },
    "investors_family_office": {
        "label": "Family Office & HNW",
        "contact_type": "investor", "investor_type": "family_office",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Chief Investment Officer", "Investment Director", "Portfolio Manager",
                                "Managing Director", "Founder", "Principal"],
                    "industries": ["Investment Management", "Family Office", "Wealth Management"]},
    },
    "investors_cvc_industrial": {
        "label": "Corporate VC — Industrials",
        "contact_type": "investor", "investor_type": "cvc",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Head of Corporate Ventures", "VP Corporate Development", "Director of Strategy",
                                "Head of Innovation", "Investment Manager", "Open Innovation Manager",
                                "Chief Strategy Officer", "Head of New Ventures"],
                    "industries": ["Chemicals", "Oil & Gas", "Mining & Metals", "Industrial Conglomerates",
                                   "Utilities", "Automotive", "Steel", "Paper & Forest Products"]},
    },
    "investors_cvc_tech": {
        "label": "Corporate VC — Tech",
        "contact_type": "investor", "investor_type": "cvc",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Head of Corporate Ventures", "VP Corporate Development", "Head of Innovation",
                                "Investment Manager", "Open Innovation Manager", "Chief Strategy Officer"],
                    "industries": ["Computer Software", "Internet", "Information Technology & Services",
                                   "Semiconductors", "Telecommunications"]},
    },
    "investors_angels": {
        "label": "Angel Investors & Advisors",
        "contact_type": "investor", "investor_type": "angel",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Angel Investor", "Entrepreneur in Residence", "Venture Advisor",
                                "Startup Advisor", "Board Member", "Board Director", "Non-Executive Director"]},
    },
    "investors_accelerators": {
        "label": "Accelerators & Incubators",
        "contact_type": "investor", "investor_type": "accelerator",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Program Director", "Managing Director", "Head of Portfolio",
                                "Ecosystem Manager", "Venture Builder"],
                    "industries": ["Venture Capital & Private Equity", "Entrepreneurship",
                                   "Program Development", "Startup Ecosystem"]},
    },

    # ---- INDUSTRIAL PROSPECTS (Symbioflows sellers) ----

    "sellers_sustainability_all": {
        "label": "Sustainability & ESG — all heavy industry",
        "contact_type": "prospect", "prospect_side": "seller",
        "venture_tags": ["symbioflows"],
        "filters": {"titles": ["Chief Sustainability Officer", "Head of Sustainability", "Director of Sustainability",
                                "Sustainability Manager", "ESG Manager", "Environmental Manager",
                                "Circular Economy Manager", "Waste Manager", "Head of Environment",
                                "Environmental Compliance Manager", "HSE Director", "EHS Manager"]},
    },
    "sellers_operations_chemicals": {
        "label": "Operations — Chemicals & Oil & Gas",
        "contact_type": "prospect", "prospect_side": "seller",
        "venture_tags": ["symbioflows"],
        "filters": {"titles": ["VP Operations", "Director of Operations", "Plant Manager", "Facility Manager",
                                "HSE Manager", "Head of Operations", "Operations Director", "Site Manager",
                                "Production Manager", "Process Manager"],
                    "industries": ["Chemicals", "Oil & Gas", "Petrochemicals"]},
    },
    "sellers_operations_metals": {
        "label": "Operations — Mining & Metals & Steel",
        "contact_type": "prospect", "prospect_side": "seller",
        "venture_tags": ["symbioflows"],
        "filters": {"titles": ["VP Operations", "Director of Operations", "Plant Manager", "Facility Manager",
                                "Head of Operations", "Operations Director", "Site Manager", "Production Manager"],
                    "industries": ["Mining & Metals", "Steel", "Metals"]},
    },
    "sellers_operations_manufacturing": {
        "label": "Operations — Manufacturing & Automotive",
        "contact_type": "prospect", "prospect_side": "seller",
        "venture_tags": ["symbioflows"],
        "filters": {"titles": ["VP Operations", "Director of Operations", "Plant Manager", "Facility Manager",
                                "Head of Operations", "Operations Director", "Site Manager", "Production Manager"],
                    "industries": ["Automotive", "Manufacturing", "Industrial Machinery", "Plastics"]},
    },
    "sellers_operations_food": {
        "label": "Operations — Food, Paper & Utilities",
        "contact_type": "prospect", "prospect_side": "seller",
        "venture_tags": ["symbioflows"],
        "filters": {"titles": ["VP Operations", "Director of Operations", "Plant Manager", "Facility Manager",
                                "Head of Operations", "Operations Director", "Site Manager"],
                    "industries": ["Food & Beverages", "Paper & Forest Products", "Utilities", "Packaging"]},
    },

    # ---- BUYERS (feedstock sourcing) ----

    "buyers_procurement_all": {
        "label": "Procurement & Supply Chain — all industries",
        "contact_type": "prospect", "prospect_side": "buyer",
        "venture_tags": ["symbioflows"],
        "filters": {"titles": ["Chief Procurement Officer", "CPO", "Head of Procurement", "VP Procurement",
                                "Director of Procurement", "Procurement Manager", "Head of Supply Chain",
                                "VP Supply Chain", "Raw Materials Manager", "Category Manager",
                                "Head of Sourcing", "Strategic Sourcing Manager", "Head of Raw Materials"]},
    },
    "buyers_ceo_industrial": {
        "label": "CEOs — Industrial companies",
        "contact_type": "prospect", "prospect_side": "buyer",
        "venture_tags": ["symbioflows", "all_ventures"],
        "filters": {"titles": ["CEO", "Chief Executive Officer", "Managing Director", "President"],
                    "industries": ["Chemicals", "Mining & Metals", "Oil & Gas", "Steel",
                                   "Manufacturing", "Automotive", "Plastics", "Paper & Forest Products"]},
    },

    # ---- TECH & SAAS (future ventures) ----

    "tech_founders": {
        "label": "Tech Founders & Co-Founders",
        "contact_type": "prospect",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Founder", "Co-Founder", "CEO", "CTO"],
                    "industries": ["Computer Software", "Internet", "Information Technology & Services",
                                   "Artificial Intelligence", "SaaS", "Data & Analytics"]},
    },
    "tech_cxo": {
        "label": "Tech CXOs",
        "contact_type": "prospect",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Chief Technology Officer", "CTO", "Chief Product Officer", "CPO",
                                "Chief Data Officer", "CDO", "VP Engineering", "VP Product"],
                    "industries": ["Computer Software", "Internet", "Information Technology & Services"]},
    },

    # ---- FINANCE (future fintech/marketplace ventures) ----

    "finance_cfo": {
        "label": "CFOs & Finance Directors — all industries",
        "contact_type": "prospect",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Chief Financial Officer", "CFO", "Finance Director", "VP Finance",
                                "Head of Finance", "Financial Controller"]},
    },
    "finance_banking": {
        "label": "Banking & Financial Services",
        "contact_type": "prospect",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Managing Director", "Director", "VP", "Head of"],
                    "industries": ["Banking", "Financial Services", "Investment Banking",
                                   "Capital Markets", "Asset Management"]},
    },

    # ---- LOGISTICS & SUPPLY CHAIN ----

    "logistics_leaders": {
        "label": "Logistics & Supply Chain Leaders",
        "contact_type": "prospect",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Head of Logistics", "VP Logistics", "Director of Supply Chain",
                                "Chief Supply Chain Officer", "CSCO", "Head of Distribution",
                                "VP Transportation", "Head of Freight"],
                    "industries": ["Logistics & Supply Chain", "Transportation/Trucking/Railroad",
                                   "Warehousing", "Import & Export"]},
    },

    # ---- CLIMATE TECH & ENERGY ----

    "climate_energy": {
        "label": "Climate Tech & Renewable Energy",
        "contact_type": "prospect",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["CEO", "Founder", "Co-Founder", "Managing Director", "Director",
                                "Head of Business Development", "VP Business Development"],
                    "industries": ["Renewables & Environment", "Clean Energy", "Environmental Services",
                                   "Cleantech", "Climate Tech"]},
    },

    # ---- PRESS, ANALYSTS & ACADEMIA ----

    "press_tech_industrial": {
        "label": "Press & Analysts — tech and industrial beat",
        "contact_type": "press",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Journalist", "Senior Reporter", "Editor", "Senior Editor",
                                "Managing Editor", "Editor in Chief", "Bureau Chief"]},
    },
    "analysts_research": {
        "label": "Research Analysts & Industry Analysts",
        "contact_type": "press",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Research Analyst", "Senior Research Analyst", "Industry Analyst",
                                "Senior Analyst", "Principal Analyst", "Research Director",
                                "Market Research Analyst", "Strategy Analyst"]},
    },
    "academia_industrial": {
        "label": "Academia — industrial sustainability & supply chain",
        "contact_type": "press",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Professor", "Associate Professor", "Assistant Professor",
                                "Research Director", "Department Head"],
                    "industries": ["Higher Education", "Research", "Think Tanks"]},
    },

    # ---- GOVERNMENT & POLICY ----

    "government_industry": {
        "label": "Government & Policy — industrial & climate",
        "contact_type": "prospect",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["Director General", "Deputy Director", "Head of Policy",
                                "Policy Director", "Commissioner", "Secretary General",
                                "Head of Department", "Chief Economist"],
                    "industries": ["Government Administration", "Public Policy", "International Affairs",
                                   "Environmental Services"]},
    },

    # ---- PHARMA & BIOTECH ----

    "pharma_ops": {
        "label": "Pharma & Biotech — operations & sustainability",
        "contact_type": "prospect",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["VP Operations", "Head of Manufacturing", "Director of Operations",
                                "Head of Sustainability", "Environmental Manager", "Head of Procurement"],
                    "industries": ["Pharmaceuticals", "Biotechnology", "Medical Devices",
                                   "Life Sciences", "Hospital & Health Care"]},
    },

    # ---- REAL ESTATE & CONSTRUCTION ----

    "real_estate_construction": {
        "label": "Real Estate & Construction leaders",
        "contact_type": "prospect",
        "venture_tags": ["all_ventures"],
        "filters": {"titles": ["CEO", "Managing Director", "Head of Development", "Development Director",
                                "COO", "Head of Sustainability", "Director of Construction"],
                    "industries": ["Real Estate", "Construction", "Civil Engineering",
                                   "Architecture & Planning"]},
    },
}


# ============================================
# DB helpers
# ============================================

def get_conn():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        database=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def _sanitize(val, col: str):
    """Coerce API response values to types psycopg2 can insert."""
    if val is None:
        return None
    if col == "raw_data":
        # JSONB: pass as Json() wrapper so psycopg2 serializes correctly
        from psycopg2.extras import Json
        return Json(val) if isinstance(val, (dict, list)) else Json(json.loads(val) if isinstance(val, str) else val)
    if col == "venture_tags":
        # PostgreSQL TEXT[]: must be a Python list
        return list(val) if isinstance(val, (list, tuple)) else [str(val)]
    if col == "email_verified":
        return bool(val)
    if isinstance(val, dict):
        # Any unexpected dict field → serialize to string
        return json.dumps(val)
    if isinstance(val, list):
        return json.dumps(val)
    return val


def upsert_contacts(records: list[dict]):
    if not records:
        return

    cols = [
        "first_name", "last_name", "email", "email_verified",
        "linkedin_url", "phone", "company", "company_domain",
        "company_size", "company_industry", "company_country", "company_city",
        "title", "seniority", "department",
        "contact_type", "investor_type", "venture_tags",
        "priority", "source", "raw_data", "import_batch",
    ]

    # Deduplicate by linkedin_url within this batch (API can return dupes)
    seen = {}
    for r in records:
        key = r.get("linkedin_url") or id(r)
        seen[key] = r
    records = list(seen.values())

    data = [tuple(_sanitize(r.get(c), c) for c in cols) for r in records]

    sql = f"""
        INSERT INTO crm.contacts ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (linkedin_url)
        WHERE linkedin_url IS NOT NULL
        DO UPDATE SET
            email          = COALESCE(EXCLUDED.email, crm.contacts.email),
            email_verified = EXCLUDED.email_verified OR crm.contacts.email_verified,
            phone          = COALESCE(EXCLUDED.phone, crm.contacts.phone),
            title          = COALESCE(EXCLUDED.title, crm.contacts.title),
            company_size   = COALESCE(EXCLUDED.company_size, crm.contacts.company_size),
            raw_data       = COALESCE(crm.contacts.raw_data, '{{}}'::jsonb) || COALESCE(EXCLUDED.raw_data, '{{}}'::jsonb),
            updated_at     = NOW()
    """

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, data, page_size=500)
        conn.commit()
        log.info(f"  Upserted {len(records)} contacts into crm.contacts")
    finally:
        conn.close()


# ============================================
# Mapping: Amplemarket response → our schema
# ============================================

def _infer_seniority(title: str) -> str:
    if not title:
        return "unknown"
    t = title.lower()
    if any(x in t for x in ["ceo", "cto", "coo", "cso", "cfo", "chief", "founder", "president", "gp", "general partner", "managing partner"]):
        return "c_suite"
    if any(x in t for x in ["vp", "vice president", "v.p."]):
        return "vp"
    if "director" in t:
        return "director"
    if any(x in t for x in ["manager", "head of", "lead", "principal"]):
        return "manager"
    return "individual"


def _infer_department(title: str) -> str:
    if not title:
        return "unknown"
    t = title.lower()
    if any(x in t for x in ["procurement", "purchasing", "supply chain", "sourcing"]):
        return "procurement"
    if any(x in t for x in ["sustainab", "esg", "environment", "climate", "circular", "csr", "hse"]):
        return "sustainability"
    if any(x in t for x in ["operation", "logistics", "warehouse", "facilities"]):
        return "operations"
    if any(x in t for x in ["invest", "partner", "principal", "portfolio", "venture", "capital"]):
        return "investment"
    if any(x in t for x in ["ceo", "coo", "founder", "president", "managing director"]):
        return "executive"
    return "other"


def _priority(contact_type: str, seniority: str) -> int:
    if contact_type == "investor" and seniority in ("c_suite", "vp"):
        return 1
    if contact_type == "investor" and seniority == "director":
        return 2
    if contact_type == "procurement" and seniority in ("c_suite", "vp"):
        return 1
    if contact_type == "procurement" and seniority == "director":
        return 2
    if seniority in ("c_suite", "vp"):
        return 2
    return 3


def map_person(person: dict, wave_cfg: dict, batch: str) -> dict:
    title = person.get("title") or person.get("job_title") or ""
    seniority = person.get("seniority") or _infer_seniority(title)
    department = _infer_department(title)
    contact_type = wave_cfg["contact_type"]

    # Company can be a nested dict object or a string depending on API endpoint
    experiences = person.get("experiences") or []
    current_exp = experiences[0] if experiences else {}
    company_raw = (person.get("company") or person.get("organization")
                   or current_exp.get("company_name"))
    if isinstance(company_raw, dict):
        company = company_raw.get("name") or company_raw.get("company_name")
    else:
        company = company_raw

    # Location is a string like "Berlin, Germany" — also in location_details
    loc_details = person.get("location_details") or {}
    country = (person.get("country") or person.get("location_country")
               or loc_details.get("country"))
    city = (person.get("city") or person.get("location_city")
            or loc_details.get("city"))

    def _s(v, maxlen=255):
        return str(v)[:maxlen] if v else None

    return {
        "first_name":       _s(person.get("first_name"), 100),
        "last_name":        _s(person.get("last_name"), 100),
        "email":            _s(person.get("email"), 255),
        "email_verified":   person.get("email_verified", False),
        "linkedin_url":     _s(person.get("linkedin_url"), 500),
        "phone":            _s(person.get("phone") or person.get("mobile_phone"), 50),
        "company":          _s(company, 255),
        "company_domain":   _s(person.get("company_domain") or person.get("website"), 255),
        "company_size":     person.get("company_size") or person.get("headcount"),
        "company_industry": person.get("industry") or person.get("company_industry"),
        "company_country":  country,
        "company_city":     city,
        "title":            title,
        "seniority":        seniority,
        "department":       department,
        "contact_type":     contact_type,
        "investor_type":    wave_cfg.get("investor_type"),
        "venture_tags":     wave_cfg.get("venture_tags", ["symbio"]),
        "priority":         _priority(contact_type, seniority),
        "source":           "amplemarket",
        "raw_data":         json.dumps(person),
        "import_batch":     batch,
    }


# ============================================
# Pipeline runners
# ============================================

def run_wave_search_only(client: AmplemarketClient, wave_name: str, batch_prefix: str):
    """
    FREE PHASE: Search people, store everything we get without spending credits.
    LinkedIn URLs, names, titles, companies — all free.
    Run this on ALL waves first to build your ranked list.
    """
    wave = SEARCH_WAVES[wave_name]
    batch = f"{batch_prefix}_{wave_name}_search"
    log.info(f"\n=== [FREE] Search Wave: {wave['label']} ===")

    people = client.search_people(wave["filters"])
    log.info(f"Found {len(people)} people (no credits spent)")

    if not people:
        return 0

    records = [map_person(p, wave, batch) for p in people]
    upsert_contacts(records)
    log.info(f"Stored {len(records)} contacts (no email yet — priority scored, pending enrichment)")
    return len(records)


def run_wave_enrich(client: AmplemarketClient, wave_name: str, batch_prefix: str,
                    credit_limit: int = 250, min_priority: int = 2):
    """
    CREDIT PHASE: Enrich only top-priority contacts we found in search phase.
    Respects credit_limit — stops when budget is hit.
    Only enriches contacts at or above min_priority.
    """
    wave = SEARCH_WAVES[wave_name]
    batch = f"{batch_prefix}_{wave_name}"
    log.info(f"\n=== [CREDITS] Enrich Wave: {wave['label']} (budget: {credit_limit} emails, priority <= {min_priority}) ===")

    # Pull contacts from this wave that need enrichment
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, first_name, last_name, linkedin_url, company, company_domain, title
            FROM crm.contacts
            WHERE import_batch LIKE %s
              AND email IS NULL
              AND linkedin_url IS NOT NULL
              AND priority <= %s
            ORDER BY priority ASC, id ASC
            LIMIT %s
        """, (f"%{wave_name}%", min_priority, credit_limit))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        log.info("No contacts pending enrichment for this wave.")
        return

    log.info(f"Enriching {len(rows)} contacts (within credit budget of {credit_limit})")

    leads = [{"linkedin_url": r[2]} for r in rows]
    id_map = {r[2]: r[0] for r in rows}  # linkedin_url → db id

    request_id = client.submit_people_enrichment(leads)

    total_enriched = 0
    conn = get_conn()
    try:
        for page in client.poll_people_enrichment(request_id):
            for enriched in page:
                linkedin = enriched.get("linkedin_url")
                email = enriched.get("email")
                phone = enriched.get("phone")
                if not linkedin:
                    continue
                db_id = id_map.get(linkedin)
                if not db_id:
                    continue
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE crm.contacts
                        SET email = COALESCE(%s, email),
                            email_verified = %s,
                            phone = COALESCE(%s, phone),
                            raw_data = raw_data || %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (
                        email,
                        enriched.get("email_verified", False),
                        phone,
                        json.dumps(enriched),
                        db_id,
                    ))
                total_enriched += 1
            conn.commit()
            log.info(f"  Enriched {total_enriched} contacts so far...")
    finally:
        conn.close()

    log.info(f"Enrichment complete: {total_enriched} contacts now have emails.")


def run_wave(client: AmplemarketClient, wave_name: str, batch_prefix: str):
    """
    Legacy combined run: search + enrich in one shot.
    Prefer run_wave_search_only first, then run_wave_enrich separately.
    """
    run_wave_search_only(client, wave_name, batch_prefix)
    credit_budget = CREDIT_BUDGETS.get(wave_name, 100)
    run_wave_enrich(client, wave_name, batch_prefix, credit_limit=credit_budget)


def run_eprtr_enrichment(client: AmplemarketClient, batch_prefix: str, limit: int = 5000):
    """
    The moat hack: pull your own industrial companies from E-PRTR data,
    enrich them via Amplemarket to get decision-maker contacts.
    """
    log.info("\n=== E-PRTR Moat Hack: Enriching your own industrial company list ===")
    batch = f"{batch_prefix}_eprtr_enrichment"

    # Pull top companies from your waste listings
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                source_company AS name,
                source_country AS country,
                source_industry AS industry,
                MAX(quantity_tons) AS max_tons
            FROM waste_listings
            WHERE source_company IS NOT NULL
              AND LENGTH(source_company) > 3
            GROUP BY source_company, source_country, source_industry
            ORDER BY MAX(quantity_tons) DESC NULLS LAST
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()

    log.info(f"Pulled {len(rows)} companies from E-PRTR data")

    # Submit company enrichment to get domains + LinkedIn URLs
    companies = [{"name": r[0], "country": r[1]} for r in rows if r[0]]

    # Chunk into 10k batches
    for i in range(0, len(companies), 10_000):
        chunk = companies[i:i + 10_000]
        log.info(f"Submitting company enrichment batch {i//10_000 + 1} ({len(chunk)} companies)...")
        request_id = client.submit_company_enrichment(chunk)

        enriched_companies = []
        for page in client.poll_company_enrichment(request_id):
            enriched_companies.extend(page)

        log.info(f"Got {len(enriched_companies)} enriched companies")

        # Now find people at these companies
        domains = [c.get("domain") for c in enriched_companies if c.get("domain")]
        if not domains:
            log.warning("No domains found in company enrichment — skipping people search")
            continue

        # Search for procurement + sustainability contacts at these specific companies
        for title_group, contact_type in [
            (["Head of Procurement", "CPO", "Director of Procurement",
              "VP Supply Chain", "Purchasing Manager"], "procurement"),
            (["Head of Sustainability", "CSO", "ESG Manager",
              "Environmental Manager", "Waste Manager"], "prospect"),
        ]:
            log.info(f"Finding {contact_type} contacts at enriched companies...")
            people = client.search_people({
                "titles": title_group,
                "company_domains": domains[:500],  # API may have limits
            })

            if not people:
                continue

            leads = [{"linkedin_url": p["linkedin_url"]} for p in people if p.get("linkedin_url")]
            if not leads:
                continue

            request_id2 = client.submit_people_enrichment(leads)
            wave_cfg = {
                "contact_type": contact_type,
                "venture_tags": ["symbio"],
                "investor_type": None,
            }
            originals = {p.get("linkedin_url", ""): p for p in people}

            total = 0
            for page in client.poll_people_enrichment(request_id2):
                records = []
                for enriched in page:
                    original = originals.get(enriched.get("linkedin_url", ""), {})
                    merged = {**original, **enriched}
                    records.append(map_person(merged, wave_cfg, batch))
                upsert_contacts(records)
                total += len(records)

            log.info(f"  Stored {total} {contact_type} contacts from E-PRTR companies")


# ============================================
# Entry point
# ============================================

# ============================================
# Regional splits — breaks through the 10k per-search cap
# Each region is a separate API call so you get up to 10k per region
# ============================================

REGIONS = {
    "us":    {"label": "US & Canada",
              "locations": ["United States", "Canada"]},
    "eu":    {"label": "Europe",
              "locations": ["United Kingdom", "Germany", "France", "Netherlands",
                            "Sweden", "Spain", "Italy", "Switzerland", "Belgium",
                            "Denmark", "Norway", "Finland", "Austria", "Portugal",
                            "Ireland", "Poland", "Czech Republic", "Romania", "Hungary"]},
    "mena":  {"label": "MENA",
              "locations": ["United Arab Emirates", "Saudi Arabia", "Qatar", "Kuwait",
                            "Bahrain", "Oman", "Egypt", "Jordan", "Israel",
                            "Morocco", "Tunisia", "Lebanon"]},
    "apac":  {"label": "APAC",
              "locations": ["Australia", "Singapore", "India", "Japan", "South Korea",
                            "Hong Kong", "New Zealand", "Malaysia", "Indonesia",
                            "Philippines", "Vietnam", "Thailand", "China", "Taiwan"]},
    "latam": {"label": "LATAM",
              "locations": ["Brazil", "Mexico", "Colombia", "Argentina", "Chile",
                            "Peru", "Venezuela", "Ecuador", "Uruguay", "Panama"]},
}

# Waves to split regionally — all major ones so we capture contacts
# that fell outside the 10k global cap
REGIONAL_BASE_WAVES = [
    "investors_vc_partners",
    "investors_vc_principals",
    "investors_pe",
    "investors_family_office",
    "investors_cvc_industrial",
    "investors_cvc_tech",
    "investors_accelerators",
    "investors_angels",
    "sellers_sustainability_all",
    "sellers_operations_chemicals",
    "sellers_operations_metals",
    "sellers_operations_manufacturing",
    "sellers_operations_food",
    "buyers_procurement_all",
    "buyers_ceo_industrial",
    "tech_founders",
    "tech_cxo",
    "finance_cfo",
    "finance_banking",
    "logistics_leaders",
    "climate_energy",
    "press_tech_industrial",
    "analysts_research",
    "academia_industrial",
    "pharma_ops",
    "government_industry",
]

# Generate regional variants and inject into SEARCH_WAVES
# Key format: "investors_vc_partners__us", "investors_vc_partners__eu", etc.
for _base_key in REGIONAL_BASE_WAVES:
    _base = SEARCH_WAVES[_base_key]
    for _region_key, _region in REGIONS.items():
        _wave_key = f"{_base_key}__{_region_key}"
        _wave = {
            **_base,
            "label": f"{_base['label']} — {_region['label']}",
            "filters": {
                **_base["filters"],
                "locations": _region["locations"],
            },
        }
        SEARCH_WAVES[_wave_key] = _wave


# Credit budgets per mode — prevents accidental overspend
# Total email credits: 1,250. Be deliberate.
CREDIT_BUDGETS = {
    "investors":   250,   # VCs + angels + CVCs
    "procurement": 700,   # CPO/VP level procurement
    "prospects":   200,   # sustainability directors
    "press":        50,   # journalists/analysts
    "eprtr":       300,   # moat hack — enriching your own companies
    "all":        1200,   # leave 50 as buffer
}

MODE_MAP = {
    # Original global waves (no location filter)
    "investors":    [k for k in SEARCH_WAVES if k.startswith("investors_") and "__" not in k],
    "sellers":      [k for k in SEARCH_WAVES if k.startswith("sellers_") and "__" not in k],
    "buyers":       [k for k in SEARCH_WAVES if k.startswith("buyers_") and "__" not in k],
    "press":        [k for k in SEARCH_WAVES if ("press" in k or "analysts" in k or "academia" in k) and "__" not in k],
    "tech":         [k for k in SEARCH_WAVES if k.startswith("tech_") and "__" not in k],
    "finance":      [k for k in SEARCH_WAVES if k.startswith("finance_") and "__" not in k],
    "all":          [k for k in SEARCH_WAVES if "__" not in k],

    # Regional variants — each region runs all major waves geo-filtered
    "regional":         [k for k in SEARCH_WAVES if "__" in k],
    "regional_us":      [k for k in SEARCH_WAVES if k.endswith("__us")],
    "regional_eu":      [k for k in SEARCH_WAVES if k.endswith("__eu")],
    "regional_mena":    [k for k in SEARCH_WAVES if k.endswith("__mena")],
    "regional_apac":    [k for k in SEARCH_WAVES if k.endswith("__apac")],
    "regional_latam":   [k for k in SEARCH_WAVES if k.endswith("__latam")],

    # Regional by category (e.g. just investor waves in Europe)
    "investors_regional":   [k for k in SEARCH_WAVES if k.startswith("investors_") and "__" in k],
    "sellers_regional":     [k for k in SEARCH_WAVES if k.startswith("sellers_") and "__" in k],
    "buyers_regional":      [k for k in SEARCH_WAVES if k.startswith("buyers_") and "__" in k],
}


def show_pending_enrichment():
    """Show how many contacts are awaiting enrichment by priority tier."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                contact_type,
                priority,
                COUNT(*) AS total,
                COUNT(email) AS has_email,
                COUNT(*) - COUNT(email) AS needs_email
            FROM crm.contacts
            GROUP BY contact_type, priority
            ORDER BY contact_type, priority
        """)
        rows = cur.fetchall()
    conn.close()

    print(f"\n{'Type':<15} {'P':>2}  {'Total':>6}  {'Has Email':>10}  {'Needs Email':>12}")
    print("-" * 55)
    for r in rows:
        print(f"{r[0]:<15} {r[1]:>2}  {r[2]:>6}  {r[3]:>10}  {r[4]:>12}")

    # Estimate credit cost
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM crm.contacts
            WHERE email IS NULL AND priority <= 2
        """)
        high_priority_without_email = cur.fetchone()[0]
    conn.close()
    print(f"\nHigh-priority contacts needing email (P1+P2): {high_priority_without_email}")
    print(f"Estimated credits needed: {high_priority_without_email} email credits")


def main():
    parser = argparse.ArgumentParser(
        description="Amplemarket pipeline — search is FREE, enrichment costs credits",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Two-phase workflow (recommended to preserve credits):

  Phase 1 — FREE: search everything, store LinkedIn URLs + metadata
    python -m amplemarket.pipeline --search --mode all --batch march_2026

  Phase 2 — CREDITS: enrich only top priority contacts
    python -m amplemarket.pipeline --enrich --mode investors --budget 250
    python -m amplemarket.pipeline --enrich --mode procurement --budget 700

  Check what needs enrichment:
    python -m amplemarket.pipeline --status

  E-PRTR moat hack (turn your industrial data into contacts):
    python -m amplemarket.pipeline --eprtr --limit 5000
        """
    )
    parser.add_argument("--search", action="store_true",
                        help="Phase 1: search only, no credits spent")
    parser.add_argument("--enrich", action="store_true",
                        help="Phase 2: enrich top-priority contacts (costs email credits)")
    parser.add_argument("--mode", choices=list(MODE_MAP.keys()) + list(SEARCH_WAVES.keys()),
                        default="all", help="Which waves to run. Use 'regional' to run all geo-splits, 'regional_eu' for Europe only, etc.")
    parser.add_argument("--budget", type=int, default=None,
                        help="Max email credits to spend (overrides defaults)")
    parser.add_argument("--priority", type=int, default=2,
                        help="Only enrich contacts with priority <= this (default: 2)")
    parser.add_argument("--eprtr", action="store_true",
                        help="Run E-PRTR moat enrichment")
    parser.add_argument("--limit", type=int, default=5000,
                        help="Max companies for E-PRTR enrichment")
    parser.add_argument("--batch", default="march_2026",
                        help="Batch label prefix")
    parser.add_argument("--status", action="store_true",
                        help="Show enrichment status — how many contacts need emails")
    args = parser.parse_args()

    if args.status:
        show_pending_enrichment()
        return

    api_key = os.getenv("AMPLEMARKET_API_KEY")
    if not api_key:
        log.error("AMPLEMARKET_API_KEY not set in .env")
        sys.exit(1)

    client = AmplemarketClient(api_key)

    if args.eprtr:
        run_eprtr_enrichment(client, args.batch, limit=args.limit)
        return

    waves = MODE_MAP.get(args.mode, [args.mode])

    if args.search:
        # Phase 1: free search across all waves
        total = 0
        for wave_name in waves:
            n = run_wave_search_only(client, wave_name, args.batch)
            total += n
            time.sleep(1)
        log.info(f"\nSearch complete. {total} contacts stored. No credits spent.")
        log.info("Run --status to see enrichment queue, then --enrich to spend credits.")
        return

    if args.enrich:
        # Phase 2: enrich only what we've already searched
        for wave_name in waves:
            budget = args.budget or CREDIT_BUDGETS.get(wave_name, 100)
            run_wave_enrich(client, wave_name, args.batch,
                            credit_limit=budget, min_priority=args.priority)
            time.sleep(2)
        return

    # Default: show help if neither flag given
    parser.print_help()


if __name__ == "__main__":
    main()
