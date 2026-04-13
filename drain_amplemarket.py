#!/usr/bin/env python3
"""
drain_amplemarket.py
=====================
EMERGENCY: subscription expires tomorrow.
Suck every data point out of Amplemarket before it ends.

Strategy:
  Phase 1 - FREE: Run every search wave, dump raw JSON + CSV locally.
             No credits spent. Get: names, titles, companies, LinkedIn URLs.
  Phase 2 - CREDITS: Bulk-enrich all Priority 1-2 contacts from Phase 1.
             Get: verified emails, phones, company domains, headcount.
  Phase 3 - EXPORT: Flatten everything to final CSV for CRM/outreach use.

Usage:
  python drain_amplemarket.py --phase search     # Phase 1 only (free, safe to run first)
  python drain_amplemarket.py --phase enrich     # Phase 2 (burns credits)
  python drain_amplemarket.py --phase export     # Phase 3 (merge + deduplicate all files)
  python drain_amplemarket.py --phase all        # Run everything back to back
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent))
from amplemarket.client import AmplemarketClient

# ---------------------------------------------------------------
# Logging — write to both console and file so we can tail it
# ---------------------------------------------------------------
LOG_FILE = Path("drain_amplemarket.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------
# Config
# ---------------------------------------------------------------
API_KEY = os.getenv("AMPLEMARKET_API_KEY")
if not API_KEY:
    sys.exit("ERROR: AMPLEMARKET_API_KEY not set in .env")

OUT_DIR = Path("exports/amplemarket_drain")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M")

# ---------------------------------------------------------------
# ALL search waves — every ICP we care about
# ---------------------------------------------------------------
SEARCH_WAVES = {

    # ---- INVESTORS ----

    "inv_vc_partners": {
        "label": "VC General Partners",
        "contact_type": "investor",
        "filters": {"titles": ["General Partner", "Managing Partner", "Partner", "Venture Partner", "Investment Partner"]},
    },
    "inv_vc_principals": {
        "label": "VC Principals & Directors",
        "contact_type": "investor",
        "filters": {"titles": ["Principal", "Investment Director", "Associate", "Senior Associate", "Vice President",
                               "Investment Associate"]},
    },
    "inv_pe": {
        "label": "Private Equity",
        "contact_type": "investor",
        "filters": {"titles": ["Managing Director", "Partner", "Principal", "Director", "Investment Manager"],
                    "industries": ["Private Equity", "Investment Management", "Venture Capital & Private Equity"]},
    },
    "inv_family_office": {
        "label": "Family Office & HNW",
        "contact_type": "investor",
        "filters": {"titles": ["Chief Investment Officer", "Investment Director", "Portfolio Manager",
                               "Managing Director", "Founder", "Principal"],
                    "industries": ["Investment Management", "Family Office", "Wealth Management"]},
    },
    "inv_cvc_industrial": {
        "label": "Corporate VC — Industrials",
        "contact_type": "investor",
        "filters": {"titles": ["Head of Corporate Ventures", "VP Corporate Development", "Director of Strategy",
                               "Head of Innovation", "Investment Manager", "Open Innovation Manager",
                               "Chief Strategy Officer", "Head of New Ventures"],
                    "industries": ["Chemicals", "Oil & Gas", "Mining & Metals", "Industrial Conglomerates",
                                   "Utilities", "Automotive", "Steel", "Paper & Forest Products"]},
    },
    "inv_cvc_tech": {
        "label": "Corporate VC — Tech",
        "contact_type": "investor",
        "filters": {"titles": ["Head of Corporate Ventures", "VP Corporate Development", "Head of Innovation",
                               "Investment Manager", "Open Innovation Manager", "Chief Strategy Officer"],
                    "industries": ["Computer Software", "Internet", "Information Technology & Services",
                                   "Semiconductors", "Telecommunications"]},
    },
    "inv_angels": {
        "label": "Angel Investors",
        "contact_type": "investor",
        "filters": {"titles": ["Angel Investor", "Entrepreneur in Residence", "Venture Advisor",
                               "Startup Advisor", "Board Member", "Board Director", "Non-Executive Director"]},
    },
    "inv_accelerators": {
        "label": "Accelerators & Incubators",
        "contact_type": "investor",
        "filters": {"titles": ["Program Director", "Managing Director", "Head of Portfolio",
                               "Ecosystem Manager", "Venture Builder"],
                    "industries": ["Venture Capital & Private Equity", "Entrepreneurship"]},
    },

    # ---- INDUSTRIAL SELLERS (primary Symbioflows ICP) ----

    "sellers_sustainability": {
        "label": "Sustainability & ESG — heavy industry",
        "contact_type": "prospect",
        "prospect_side": "seller",
        "filters": {"titles": ["Chief Sustainability Officer", "Head of Sustainability", "Director of Sustainability",
                               "Sustainability Manager", "ESG Manager", "Environmental Manager",
                               "Circular Economy Manager", "Waste Manager", "Head of Environment",
                               "Environmental Compliance Manager", "HSE Director", "EHS Manager",
                               "Head of HSE", "VP Sustainability", "Director HSE"]},
    },
    "sellers_ops_chemicals": {
        "label": "Operations — Chemicals & Oil & Gas",
        "contact_type": "prospect",
        "prospect_side": "seller",
        "filters": {"titles": ["VP Operations", "Director of Operations", "Plant Manager", "Facility Manager",
                               "HSE Manager", "Head of Operations", "Operations Director", "Site Manager",
                               "Production Manager", "Process Manager", "VP Manufacturing"],
                    "industries": ["Chemicals", "Oil & Gas", "Petrochemicals", "Specialty Chemicals"]},
    },
    "sellers_ops_metals": {
        "label": "Operations — Mining & Metals & Steel",
        "contact_type": "prospect",
        "prospect_side": "seller",
        "filters": {"titles": ["VP Operations", "Director of Operations", "Plant Manager", "Facility Manager",
                               "Head of Operations", "Operations Director", "Site Manager",
                               "Production Manager", "VP Manufacturing", "Metallurgical Manager"],
                    "industries": ["Mining & Metals", "Steel", "Metals", "Iron & Steel"]},
    },
    "sellers_ops_manufacturing": {
        "label": "Operations — Manufacturing & Automotive",
        "contact_type": "prospect",
        "prospect_side": "seller",
        "filters": {"titles": ["VP Operations", "Director of Operations", "Plant Manager", "Facility Manager",
                               "Head of Operations", "Operations Director", "Site Manager", "Production Manager"],
                    "industries": ["Automotive", "Manufacturing", "Industrial Machinery", "Plastics",
                                   "Rubber & Plastics"]},
    },
    "sellers_ops_food": {
        "label": "Operations — Food, Paper & Utilities",
        "contact_type": "prospect",
        "prospect_side": "seller",
        "filters": {"titles": ["VP Operations", "Director of Operations", "Plant Manager", "Facility Manager",
                               "Head of Operations", "Operations Director", "Site Manager"],
                    "industries": ["Food & Beverages", "Paper & Forest Products", "Utilities",
                                   "Packaging", "Food Production"]},
    },
    "sellers_mena": {
        "label": "MENA Industrial Leaders",
        "contact_type": "prospect",
        "prospect_side": "seller",
        "filters": {"titles": ["CEO", "COO", "Managing Director", "Plant Manager", "VP Operations",
                               "Head of Sustainability", "General Manager", "Country Manager"],
                    "industries": ["Chemicals", "Oil & Gas", "Mining & Metals", "Manufacturing",
                                   "Construction", "Utilities"],
                    "locations": ["United Arab Emirates", "Saudi Arabia", "Kuwait", "Qatar",
                                  "Bahrain", "Oman", "Egypt", "Morocco", "Jordan"]},
    },

    # ---- BUYERS (feedstock sourcing) ----

    "buyers_procurement": {
        "label": "Procurement & Supply Chain — all industries",
        "contact_type": "prospect",
        "prospect_side": "buyer",
        "filters": {"titles": ["Chief Procurement Officer", "CPO", "Head of Procurement", "VP Procurement",
                               "Director of Procurement", "Procurement Manager", "Head of Supply Chain",
                               "VP Supply Chain", "Raw Materials Manager", "Category Manager",
                               "Head of Sourcing", "Strategic Sourcing Manager", "Head of Raw Materials",
                               "VP Sourcing", "Global Category Manager"]},
    },
    "buyers_ceo_industrial": {
        "label": "CEOs — Industrial companies",
        "contact_type": "prospect",
        "prospect_side": "buyer",
        "filters": {"titles": ["CEO", "Chief Executive Officer", "Managing Director", "President"],
                    "industries": ["Chemicals", "Mining & Metals", "Oil & Gas", "Steel",
                                   "Manufacturing", "Automotive", "Plastics", "Paper & Forest Products"]},
    },
    "buyers_procurement_mena": {
        "label": "MENA Procurement",
        "contact_type": "prospect",
        "prospect_side": "buyer",
        "filters": {"titles": ["CPO", "Head of Procurement", "Director of Procurement", "VP Procurement",
                               "Head of Supply Chain", "Category Manager", "Procurement Manager"],
                    "locations": ["United Arab Emirates", "Saudi Arabia", "Kuwait", "Qatar", "Bahrain"]},
    },

    # ---- PRESS & ANALYSTS ----

    "press_industrial": {
        "label": "Press & Analysts — industrial beat",
        "contact_type": "press",
        "filters": {"titles": ["Journalist", "Senior Reporter", "Editor", "Senior Editor",
                               "Managing Editor", "Editor in Chief", "Bureau Chief",
                               "Industry Reporter", "Environment Correspondent"]},
    },
    "analysts_research": {
        "label": "Research Analysts — industry",
        "contact_type": "press",
        "filters": {"titles": ["Research Analyst", "Senior Research Analyst", "Industry Analyst",
                               "Senior Analyst", "Principal Analyst", "Research Director",
                               "Market Research Analyst"],
                    "industries": ["Research", "Management Consulting", "Chemicals",
                                   "Environmental Services", "Think Tanks"]},
    },

    # ---- GOVERNMENT & POLICY ----

    "gov_industrial_policy": {
        "label": "Government — industrial & climate policy",
        "contact_type": "prospect",
        "filters": {"titles": ["Director General", "Deputy Director", "Head of Policy",
                               "Policy Director", "Commissioner", "Secretary General",
                               "Head of Department", "Chief Economist", "Regulatory Affairs Director"],
                    "industries": ["Government Administration", "Public Policy", "International Affairs",
                                   "Environmental Services", "Regulatory Affairs"]},
    },

    # ---- CLIMATE TECH ----

    "climate_energy": {
        "label": "Climate Tech & Renewables",
        "contact_type": "prospect",
        "filters": {"titles": ["CEO", "Founder", "Co-Founder", "Managing Director",
                               "Head of Business Development", "VP Business Development"],
                    "industries": ["Renewables & Environment", "Clean Energy", "Environmental Services",
                                   "Cleantech", "Climate Tech", "Clean Technology"]},
    },

    # ---- TECH & SAAS ----

    "tech_founders": {
        "label": "Tech Founders",
        "contact_type": "prospect",
        "filters": {"titles": ["Founder", "Co-Founder", "CEO", "CTO"],
                    "industries": ["Computer Software", "Internet", "Information Technology & Services",
                                   "Artificial Intelligence", "SaaS", "Data & Analytics",
                                   "Enterprise Software"]},
    },

    # ---- FINANCE ----

    "finance_cfo": {
        "label": "CFOs & Finance Directors",
        "contact_type": "prospect",
        "filters": {"titles": ["Chief Financial Officer", "CFO", "Finance Director", "VP Finance",
                               "Head of Finance", "Financial Controller"]},
    },

    # ---- PHARMA & BIOTECH (byproduct angle) ----

    "pharma_ops": {
        "label": "Pharma & Biotech — ops & sustainability",
        "contact_type": "prospect",
        "filters": {"titles": ["VP Operations", "Head of Manufacturing", "Director of Operations",
                               "Head of Sustainability", "Environmental Manager", "Head of Procurement"],
                    "industries": ["Pharmaceuticals", "Biotechnology", "Medical Devices", "Life Sciences"]},
    },

    # ---- LOGISTICS ----

    "logistics_leaders": {
        "label": "Logistics & Supply Chain Leaders",
        "contact_type": "prospect",
        "filters": {"titles": ["Head of Logistics", "VP Logistics", "Director of Supply Chain",
                               "Chief Supply Chain Officer", "CSCO", "Head of Distribution",
                               "VP Transportation"],
                    "industries": ["Logistics & Supply Chain", "Transportation/Trucking/Railroad",
                                   "Warehousing", "Import & Export", "Freight"]},
    },

    # ---- NEW WAVES ----

    "sellers_construction": {
        "label": "Construction & Engineering — waste generators",
        "contact_type": "prospect",
        "prospect_side": "seller",
        "filters": {"titles": ["VP Operations", "Head of Operations", "Director of Operations",
                               "Plant Manager", "Site Manager", "Head of Sustainability",
                               "Environmental Manager", "HSE Director", "Head of HSE",
                               "Head of Procurement", "Director of Procurement"],
                    "industries": ["Construction", "Civil Engineering", "Architecture & Planning",
                                   "Building Materials", "Real Estate Development"]},
    },
    "sellers_water_waste": {
        "label": "Water Treatment & Waste Services",
        "contact_type": "prospect",
        "prospect_side": "seller",
        "filters": {"titles": ["CEO", "Managing Director", "COO", "VP Operations",
                               "Head of Operations", "Director of Operations", "Plant Manager",
                               "Head of Sustainability", "Head of Business Development",
                               "VP Business Development", "General Manager"],
                    "industries": ["Environmental Services", "Utilities", "Waste Management",
                                   "Water Supply & Irrigation Systems", "Recycling",
                                   "Renewables & Environment"]},
    },
    "buyers_cement_building": {
        "label": "Cement & Building Materials — industrial byproduct buyers",
        "contact_type": "prospect",
        "prospect_side": "buyer",
        "filters": {"titles": ["CEO", "COO", "Managing Director", "VP Operations",
                               "Head of Procurement", "Director of Procurement", "Plant Manager",
                               "Head of Raw Materials", "VP Manufacturing", "Production Manager",
                               "Head of Supply Chain", "Category Manager"],
                    "industries": ["Building Materials", "Cement", "Glass", "Ceramics",
                                   "Construction Materials", "Lime & Gypsum"]},
    },
    "buyers_agri_fertilizer": {
        "label": "Agriculture & Fertilizer — nutrient recovery buyers",
        "contact_type": "prospect",
        "prospect_side": "buyer",
        "filters": {"titles": ["CEO", "COO", "Managing Director", "VP Operations",
                               "Head of Procurement", "Director of Procurement",
                               "Head of Raw Materials", "VP Supply Chain",
                               "Plant Manager", "Production Director"],
                    "industries": ["Agriculture", "Fertilizers", "Farming",
                                   "Agricultural Chemicals", "Crop Science"]},
    },
    "sellers_rubber_plastics": {
        "label": "Rubber & Plastics — byproduct sellers",
        "contact_type": "prospect",
        "prospect_side": "seller",
        "filters": {"titles": ["VP Operations", "Director of Operations", "Plant Manager",
                               "Head of Operations", "Production Manager",
                               "Head of Sustainability", "Environmental Manager",
                               "HSE Manager", "Head of Procurement"],
                    "industries": ["Plastics", "Rubber & Plastics", "Rubber Products",
                                   "Polymer Science", "Packaging", "Injection Molding"]},
    },
    "inv_sovereign_wealth": {
        "label": "Sovereign Wealth & MENA State Funds",
        "contact_type": "investor",
        "filters": {"titles": ["Investment Director", "Portfolio Manager", "Fund Manager",
                               "Chief Investment Officer", "Head of Investment",
                               "Managing Director", "Principal", "Director",
                               "Head of Infrastructure", "Head of Industrials",
                               "Head of Private Equity", "Senior Investment Manager"],
                    "industries": ["Investment Management", "Sovereign Wealth Fund",
                                   "Government Investment", "Family Office"],
                    "locations": ["United Arab Emirates", "Saudi Arabia", "Kuwait",
                                  "Qatar", "Bahrain", "Singapore", "Norway", "Abu Dhabi"]},
    },
    "sellers_textile_fashion": {
        "label": "Textile & Apparel — chemical/waste byproduct sellers",
        "contact_type": "prospect",
        "prospect_side": "seller",
        "filters": {"titles": ["Head of Sustainability", "VP Sustainability", "CSO",
                               "Chief Sustainability Officer", "Director of Sustainability",
                               "ESG Manager", "Head of Operations", "VP Operations",
                               "Head of Manufacturing", "Director of Manufacturing",
                               "Head of Environment", "Environmental Compliance Manager"],
                    "industries": ["Apparel & Fashion", "Textiles", "Sporting Goods",
                                   "Luxury Goods & Jewelry", "Consumer Goods"]},
    },
    "buyers_energy_recovery": {
        "label": "Waste-to-Energy & Energy Recovery",
        "contact_type": "prospect",
        "prospect_side": "buyer",
        "filters": {"titles": ["CEO", "Managing Director", "COO", "Head of Business Development",
                               "VP Business Development", "Director of Business Development",
                               "Head of Fuel Procurement", "Fuel Procurement Manager",
                               "Head of Operations", "Plant Manager", "General Manager"],
                    "industries": ["Renewables & Environment", "Utilities", "Energy Recovery",
                                   "Waste-to-Energy", "Alternative Energy",
                                   "Environmental Services", "Clean Energy"]},
    },
}


# ---------------------------------------------------------------
# Seniority & priority helpers
# ---------------------------------------------------------------

def _infer_seniority(title: str) -> str:
    if not title:
        return "unknown"
    t = title.lower()
    if any(x in t for x in ["ceo", "cto", "coo", "cso", "cfo", "chief", "founder",
                              "president", "gp", "general partner", "managing partner"]):
        return "c_suite"
    if any(x in t for x in ["vp", "vice president", "v.p."]):
        return "vp"
    if "director" in t:
        return "director"
    if any(x in t for x in ["manager", "head of", "lead", "principal"]):
        return "manager"
    return "individual"


def _priority(contact_type: str, seniority: str) -> int:
    if contact_type == "investor" and seniority in ("c_suite", "vp"):
        return 1
    if contact_type == "investor" and seniority == "director":
        return 2
    if contact_type == "prospect" and seniority in ("c_suite", "vp"):
        return 1
    if contact_type == "prospect" and seniority == "director":
        return 2
    if seniority in ("c_suite", "vp"):
        return 2
    return 3


def flatten(person: dict, wave_name: str, wave_cfg: dict) -> dict:
    """Flatten an API person record into a clean dict for CSV/JSON export."""
    title = person.get("title") or person.get("job_title") or ""
    seniority = person.get("seniority") or _infer_seniority(title)

    # ---- Person location ----
    loc = person.get("location_details") or {}
    country = str(person.get("country") or loc.get("country") or "")
    city    = str(person.get("city")    or loc.get("city")    or "")
    state   = str(loc.get("state") or "")

    # ---- Company (nested dict) ----
    co = person.get("company") or {}
    if not isinstance(co, dict):
        co = {}
    co_loc = co.get("location_details") or {}

    # company name: try nested dict first, then experiences
    experiences = person.get("experiences") or []
    current_exp = experiences[0] if experiences else {}
    company_name = (co.get("name") or current_exp.get("company_name") or "")

    # ---- Previous companies (comma-separated, max 3) ----
    prev_companies = ", ".join(
        e.get("company_name", "") for e in experiences[1:4] if e.get("company_name")
    )

    # ---- Funding ----
    funding_events = co.get("funding_events") or []
    latest_funding_amount = ""
    if funding_events:
        latest = funding_events[0]
        amt = latest.get("amount")
        latest_funding_amount = str(amt) if amt else ""

    # ---- Technologies (top 5) ----
    technologies = ", ".join((co.get("technologies") or [])[:5])

    # ---- Keywords (top 5) ----
    keywords = ", ".join((co.get("keywords") or [])[:5])

    def s(v, maxlen=255):
        return str(v)[:maxlen] if v else ""

    return {
        # ---- Person ----
        "first_name":               s(person.get("first_name"), 100),
        "last_name":                s(person.get("last_name"), 100),
        "full_name":                f"{person.get('first_name','') or ''} {person.get('last_name','') or ''}".strip(),
        "title":                    s(title, 200),
        "headline":                 s(person.get("headline"), 300),
        "seniority":                seniority,
        "gender":                   s(person.get("gender"), 20),
        "linkedin_url":             s(person.get("linkedin_url"), 500),
        "email":                    s(person.get("email")),
        "email_verified":           person.get("email_verified", False),
        "phone":                    s(person.get("phone") or person.get("mobile_phone"), 50),
        "country":                  s(country, 100),
        "city":                     s(city, 100),
        "state":                    s(state, 100),
        "current_role_start":       s(person.get("current_position_start_date"), 20),
        "prev_companies":           s(prev_companies, 500),

        # ---- Company ----
        "company":                  s(company_name, 255),
        "company_linkedin_url":     s(co.get("linkedin_url"), 500),
        "company_website":          s(co.get("website"), 255),
        "company_industry":         s(co.get("industry") or person.get("industry"), 150),
        "company_size":             s(co.get("size"), 50),
        "company_headcount":        co.get("estimated_number_of_employees") or "",
        "company_revenue":          s(co.get("estimated_revenue"), 50),
        "company_country":          s(co_loc.get("country") or co.get("location"), 100),
        "company_city":             s(co_loc.get("city"), 100),
        "company_type":             s(co.get("type"), 50),
        "company_founded":          co.get("founded_year") or "",
        "company_is_b2b":           co.get("is_b2b", ""),
        "total_funding":            co.get("total_funding") or "",
        "funding_stage":            s(co.get("latest_funding_stage"), 50),
        "funding_date":             s(co.get("latest_funding_date"), 30),
        "latest_funding_amount":    latest_funding_amount,
        "technologies":             s(technologies, 500),
        "company_keywords":         s(keywords, 500),

        # ---- Metadata ----
        "contact_type":             wave_cfg.get("contact_type", ""),
        "prospect_side":            wave_cfg.get("prospect_side", ""),
        "priority":                 _priority(wave_cfg.get("contact_type", ""), seniority),
        "wave":                     wave_name,
        "wave_label":               wave_cfg["label"],
        "source":                   "amplemarket",
        "batch_id":                 BATCH_ID,
    }


# ---------------------------------------------------------------
# Phase 1: Search (FREE)
# ---------------------------------------------------------------

def run_search_phase(client: AmplemarketClient):
    """
    Run every search wave. No credits. Save raw JSON + CSV per wave + merged.
    Returns list of all flattened contact dicts.
    """
    all_contacts = []
    seen_linkedin = set()

    # Load any existing wave files to skip already-done waves
    completed_waves = set()
    for f in OUT_DIR.glob("wave_*.json"):
        wave_name = f.stem.replace("wave_", "")
        completed_waves.add(wave_name)
        log.info(f"[SKIP] Wave '{wave_name}' already exists on disk")

    for wave_name, wave_cfg in SEARCH_WAVES.items():
        wave_file = OUT_DIR / f"wave_{wave_name}.json"

        if wave_name in completed_waves:
            # Load existing and add to merged set
            with open(wave_file, encoding="utf-8") as f:
                existing = json.load(f)
            for rec in existing:
                key = rec.get("linkedin_url") or f"{rec.get('full_name')}_{rec.get('company')}"
                if key not in seen_linkedin:
                    seen_linkedin.add(key)
                    all_contacts.append(rec)
            continue

        log.info(f"\n=== SEARCH: {wave_cfg['label']} ===")
        try:
            people = client.search_people(wave_cfg["filters"], max_pages=100)
            log.info(f"  Found {len(people)} people")
        except Exception as e:
            log.error(f"  FAILED: {e}")
            continue

        wave_contacts = []
        for person in people:
            rec = flatten(person, wave_name, wave_cfg)
            key = rec["linkedin_url"] or f"{rec['full_name']}_{rec['company']}"
            if key:
                wave_contacts.append(rec)
                if key not in seen_linkedin:
                    seen_linkedin.add(key)
                    all_contacts.append(rec)

        # Save per-wave JSON
        with open(wave_file, "w", encoding="utf-8") as f:
            json.dump(wave_contacts, f, indent=2, ensure_ascii=False)
        log.info(f"  Saved {len(wave_contacts)} contacts -> {wave_file.name} ({len(wave_contacts) - (len(all_contacts) - len(set(r.get('linkedin_url') for r in all_contacts if r.get('linkedin_url'))))} new unique)")

        time.sleep(0.5)  # gentle pause between waves

    log.info(f"\n=== SEARCH PHASE COMPLETE ===")
    log.info(f"Total unique contacts: {len(all_contacts)}")
    return all_contacts


# ---------------------------------------------------------------
# Phase 2: Enrichment (COSTS CREDITS)
# ---------------------------------------------------------------

def run_enrich_phase(client: AmplemarketClient, contacts: list[dict], max_credits: int = 5000):
    """
    Enrich top-priority contacts that have LinkedIn URL but no email.
    Stops at max_credits.
    Overwrites contact records in-place with enriched data.
    Saves enriched JSON to disk.
    """
    # Filter: has LinkedIn, no verified email, top priority
    to_enrich = [
        c for c in contacts
        if c.get("linkedin_url") and not c.get("email")
    ]
    # Sort: priority 1 first, then 2, then rest
    to_enrich.sort(key=lambda x: (x.get("priority", 9), x.get("wave", "")))
    to_enrich = to_enrich[:max_credits]

    if not to_enrich:
        log.info("No contacts need enrichment.")
        return contacts

    log.info(f"\n=== ENRICH PHASE: {len(to_enrich)} contacts (max {max_credits} credits) ===")
    log.info(f"  Priority breakdown: P1={sum(1 for c in to_enrich if c.get('priority')==1)}, "
             f"P2={sum(1 for c in to_enrich if c.get('priority')==2)}, "
             f"P3={sum(1 for c in to_enrich if c.get('priority')==3)}")

    leads = [{"linkedin_url": c["linkedin_url"]} for c in to_enrich]
    linkedin_to_idx = {c["linkedin_url"]: i for i, c in enumerate(contacts) if c.get("linkedin_url")}

    # Submit enrichment
    try:
        request_id = client.submit_people_enrichment(leads)
    except Exception as e:
        log.error(f"Enrichment submission failed: {e}")
        return contacts

    log.info(f"Enrichment request ID: {request_id}")
    log.info("Polling for results (this may take 10-30 minutes for large batches)...")

    enriched_count = 0
    try:
        for page in client.poll_people_enrichment(request_id, timeout=7200):
            for person in page:
                li_url = person.get("linkedin_url")
                if li_url and li_url in linkedin_to_idx:
                    idx = linkedin_to_idx[li_url]
                    # Merge enriched fields back
                    enriched = flatten(person, contacts[idx]["wave"], {"contact_type": contacts[idx]["contact_type"], "prospect_side": contacts[idx].get("prospect_side", ""), "label": contacts[idx]["wave_label"]})
                    contacts[idx].update({k: v for k, v in enriched.items() if v})
                    enriched_count += 1

            # Save intermediate checkpoint every page
            enrich_file = OUT_DIR / "enriched_checkpoint.json"
            with open(enrich_file, "w", encoding="utf-8") as f:
                json.dump([c for c in contacts if c.get("email")], f, indent=2, ensure_ascii=False)
            log.info(f"  Checkpoint: {enriched_count} enriched so far")
    except Exception as e:
        log.error(f"Enrichment polling error: {e}")

    log.info(f"Enrichment complete. Got emails for {enriched_count} contacts.")
    return contacts


# ---------------------------------------------------------------
# Phase 3: Export (merge + deduplicate)
# ---------------------------------------------------------------

EXPORT_FIELDS = [
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
    "wave", "wave_label", "source", "batch_id",
]


def run_export_phase(contacts: list[dict]):
    """Write final merged, deduplicated CSV + JSON exports."""
    if not contacts:
        log.warning("No contacts to export.")
        return

    # Deduplicate by linkedin_url (keep record with most data)
    seen = {}
    for c in contacts:
        key = c.get("linkedin_url") or f"{c.get('full_name')}_{c.get('company')}"
        if key not in seen:
            seen[key] = c
        else:
            # Merge: prefer non-empty values
            for k, v in c.items():
                if v and not seen[key].get(k):
                    seen[key][k] = v

    final = list(seen.values())
    final.sort(key=lambda x: (x.get("priority", 9), x.get("contact_type", ""), x.get("company", "")))

    # JSON export
    json_path = OUT_DIR / f"all_contacts_{BATCH_ID}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)

    # CSV export
    csv_path = OUT_DIR / f"all_contacts_{BATCH_ID}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EXPORT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(final)

    # Priority-1 only CSV (hot leads)
    p1_path = OUT_DIR / f"priority1_contacts_{BATCH_ID}.csv"
    p1 = [c for c in final if c.get("priority") == 1]
    if p1:
        with open(p1_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=EXPORT_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(p1)

    # Investors only CSV
    inv_path = OUT_DIR / f"investors_{BATCH_ID}.csv"
    investors = [c for c in final if c.get("contact_type") == "investor"]
    if investors:
        with open(inv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=EXPORT_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(investors)

    # Symbioflows prospects (sellers + buyers)
    symb_path = OUT_DIR / f"symbioflows_prospects_{BATCH_ID}.csv"
    prospects = [c for c in final if c.get("contact_type") == "prospect"]
    if prospects:
        with open(symb_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=EXPORT_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(prospects)

    # With-email only
    with_email = [c for c in final if c.get("email")]
    email_path = OUT_DIR / f"with_email_{BATCH_ID}.csv"
    if with_email:
        with open(email_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=EXPORT_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(with_email)

    log.info(f"\n=== EXPORT COMPLETE ===")
    log.info(f"  Total unique contacts : {len(final)}")
    log.info(f"  With email            : {len(with_email)}")
    log.info(f"  Priority 1            : {len(p1)}")
    log.info(f"  Investors             : {len(investors)}")
    log.info(f"  Symbioflows prospects : {len(prospects)}")
    log.info(f"")
    log.info(f"  Files written:")
    log.info(f"    {json_path}")
    log.info(f"    {csv_path}")
    if p1:
        log.info(f"    {p1_path}")
    if investors:
        log.info(f"    {inv_path}")
    if prospects:
        log.info(f"    {symb_path}")
    if with_email:
        log.info(f"    {email_path}")

    return final


# ---------------------------------------------------------------
# Load all previously saved wave files (for --phase export/enrich)
# ---------------------------------------------------------------

def load_all_wave_files() -> list[dict]:
    contacts = []
    seen = set()
    for f in sorted(OUT_DIR.glob("wave_*.json")):
        with open(f, encoding="utf-8") as fh:
            recs = json.load(fh)
        for rec in recs:
            key = rec.get("linkedin_url") or f"{rec.get('full_name')}_{rec.get('company')}"
            if key not in seen:
                seen.add(key)
                contacts.append(rec)
    log.info(f"Loaded {len(contacts)} unique contacts from {len(list(OUT_DIR.glob('wave_*.json')))} wave files")
    return contacts


# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Emergency Amplemarket data drain")
    parser.add_argument("--phase", choices=["search", "enrich", "export", "all"],
                        default="all", help="Which phase to run (default: all)")
    parser.add_argument("--max-credits", type=int, default=5000,
                        help="Max credits to spend on enrichment (default: 5000)")
    args = parser.parse_args()

    client = AmplemarketClient(API_KEY)

    log.info(f"=== AMPLEMARKET DRAIN === batch={BATCH_ID} phase={args.phase} ===")
    log.info(f"Output directory: {OUT_DIR.absolute()}")

    contacts = []

    if args.phase in ("search", "all"):
        contacts = run_search_phase(client)

    if args.phase in ("enrich", "all"):
        if not contacts:
            contacts = load_all_wave_files()
        contacts = run_enrich_phase(client, contacts, max_credits=args.max_credits)

    if args.phase in ("export", "all"):
        if not contacts:
            contacts = load_all_wave_files()
        run_export_phase(contacts)

    log.info("\nDone.")


if __name__ == "__main__":
    main()
