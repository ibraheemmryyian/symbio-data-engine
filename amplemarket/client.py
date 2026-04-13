"""
Amplemarket API Client
======================
Handles auth, rate limiting, pagination, and async enrichment polling.
"""

import logging
import time
from typing import Iterator
from urllib.parse import urlparse, parse_qs

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.amplemarket.com"
RETRY_AFTER_DEFAULT = 5  # seconds to wait between enrichment polls


class AmplemarketClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _get(self, path: str, params: dict = None) -> dict:
        r = self.session.get(f"{BASE_URL}{path}", params=params)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, payload: dict, retries: int = 3) -> dict:
        for attempt in range(retries):
            try:
                r = self.session.post(f"{BASE_URL}{path}", json=payload, timeout=60)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                if attempt < retries - 1:
                    wait = 2 ** attempt * 3
                    log.warning(f"Request failed ({e}), retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise

    def _patch(self, path: str, payload: dict = None) -> dict:
        r = self.session.patch(f"{BASE_URL}{path}", json=payload or {})
        r.raise_for_status()
        return r.json()

    # ----------------------------------------
    # People Search
    # ----------------------------------------

    def search_people(self, filters: dict, page_size: int = 100, max_pages: int = 10) -> list[dict]:
        """
        Search people with filters. Returns up to 10,000 results (API cap).

        Filter keys (all optional):
            titles          list[str]   e.g. ["Head of Procurement", "CPO"]
            seniorities     list[str]   e.g. ["director", "vp", "c_suite"]
            departments     list[str]   e.g. ["procurement", "operations"]
            locations       list[str]   e.g. ["United Arab Emirates", "Germany"]
            industries      list[str]   e.g. ["Chemicals", "Mining & Metals"]
            company_sizes   list[str]   e.g. ["201-500", "501-1000", "1000+"]
            keywords        list[str]   e.g. ["circular economy", "waste management"]
        """
        all_results = []
        page = 1

        while True:
            # page_size must be top-level, not nested in page object
            payload = {**filters, "page_size": page_size, "page": page}

            data = self._post("/people/search", payload)
            results = data.get("results", data.get("data", []))
            all_results.extend(results)

            pagination = data.get("_pagination", {})
            total_pages = pagination.get("total_pages", 1)
            log.info(f"  page {page}/{min(total_pages, max_pages)}: {len(results)} results (total so far: {len(all_results)})")

            if page >= min(total_pages, max_pages) or not results:
                break

            page += 1
            time.sleep(0.12)  # stay under 500/min rate limit

        return all_results

    def find_person(self, linkedin_url: str = None, email: str = None,
                    name: str = None, company: str = None) -> dict | None:
        """Find a single person by LinkedIn URL, email, or name+company."""
        params = {}
        if linkedin_url:
            params["linkedin_url"] = linkedin_url
        elif email:
            params["email"] = email
        elif name and company:
            params["name"] = name
            params["company"] = company
        else:
            raise ValueError("Need linkedin_url, email, or name+company")

        try:
            return self._get("/people/find", params)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

    # ----------------------------------------
    # Bulk People Enrichment (async)
    # ----------------------------------------

    def submit_people_enrichment(self, leads: list[dict]) -> str:
        """
        Submit up to 100,000 leads for bulk enrichment.

        Each lead dict should have at least one of:
            linkedin_url, email, or (first_name + last_name + company_domain)

        Returns request_id for polling.
        """
        if len(leads) > 100_000:
            raise ValueError("Max 100,000 leads per enrichment request")

        data = self._post("/people/enrichment-requests", {"leads": leads})
        # API returns flat {"id": ...} or nested {"data": {"id": ...}}
        inner = data.get("data", data)
        request_id = str(inner["id"])
        log.info(f"Enrichment request submitted: {request_id} ({len(leads)} leads)")
        return request_id

    def poll_people_enrichment(self, request_id: str, timeout: int = 3600) -> Iterator[list[dict]]:
        """
        Poll enrichment until complete. Yields pages of results as they arrive.
        Handles Retry-After headers automatically.
        """
        start = time.time()
        cursor = None

        while True:
            if time.time() - start > timeout:
                raise TimeoutError(f"Enrichment {request_id} timed out after {timeout}s")

            path = f"/people/enrichment-requests/{request_id}"
            params = {"page[size]": 100}
            if cursor:
                params["page[after]"] = cursor

            r = self.session.get(f"{BASE_URL}{path}", params=params)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", RETRY_AFTER_DEFAULT))
                log.info(f"  Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue

            r.raise_for_status()
            data = r.json()

            # API returns flat {"id":..,"status":..,"results":[]} or nested {"data":{...}}
            inner = data.get("data", data)
            status = inner.get("status")
            results = inner.get("results", [])

            if results:
                yield results

            # Extract next cursor from meta or _links — present when more pages exist
            meta = data.get("meta", inner.get("meta", {}))
            next_cursor = meta.get("next_cursor") or meta.get("after")
            links = data.get("_links", inner.get("_links", {}))
            # _links.next.href = "/people/enrichment-requests/123?page[after]=456"
            next_href = (links.get("next") or {}).get("href", "")
            if not next_cursor and next_href:
                qs = parse_qs(urlparse(next_href).query)
                next_cursor = (qs.get("page[after]") or [None])[0]
            has_next = bool(next_cursor)

            if status == "failed":
                raise RuntimeError(f"Enrichment {request_id} failed.")
            elif status == "completed" and not has_next:
                log.info(f"Enrichment {request_id} completed.")
                break
            elif status == "completed" and has_next:
                # Job done but more result pages to fetch — no sleep needed
                cursor = next_cursor
                log.info(f"  Paginating completed results (cursor={cursor})...")
            else:
                # Still processing — update cursor and wait
                cursor = next_cursor
                wait = int(r.headers.get("Retry-After", RETRY_AFTER_DEFAULT))
                log.info(f"  Status: {status} — polling again in {wait}s")
                time.sleep(wait)

    def cancel_enrichment(self, request_id: str):
        self._patch(f"/people/enrichment-requests/{request_id}")
        log.info(f"Enrichment {request_id} cancelled.")

    # ----------------------------------------
    # Bulk Company Enrichment (async)
    # ----------------------------------------

    def submit_company_enrichment(self, companies: list[dict]) -> str:
        """
        Submit up to 10,000 companies for bulk enrichment.

        Each company dict should have one of: linkedin_url, domain, name
        Returns request_id.
        """
        if len(companies) > 10_000:
            raise ValueError("Max 10,000 companies per enrichment request")

        data = self._post("/companies/enrichment-requests", {"companies": companies})
        request_id = data["data"]["id"]
        log.info(f"Company enrichment submitted: {request_id} ({len(companies)} companies)")
        return request_id

    def poll_company_enrichment(self, request_id: str, timeout: int = 3600) -> Iterator[list[dict]]:
        """Poll company enrichment until complete. Yields pages of results."""
        start = time.time()
        cursor = None

        while True:
            if time.time() - start > timeout:
                raise TimeoutError(f"Company enrichment {request_id} timed out")

            path = f"/companies/enrichment-requests/{request_id}"
            params = {"page[size]": 100}
            if cursor:
                params["page[after]"] = cursor

            r = self.session.get(f"{BASE_URL}{path}", params=params)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", RETRY_AFTER_DEFAULT))
                time.sleep(wait)
                continue

            r.raise_for_status()
            data = r.json()

            status = data.get("data", {}).get("status")
            results = data.get("data", {}).get("results", [])

            if results:
                yield results

            meta = data.get("meta", {})
            cursor = meta.get("next_cursor")

            if status == "completed":
                log.info(f"Company enrichment {request_id} completed.")
                break
            elif status == "failed":
                raise RuntimeError(f"Company enrichment {request_id} failed.")
            else:
                wait = int(r.headers.get("Retry-After", RETRY_AFTER_DEFAULT))
                time.sleep(wait)

    # ----------------------------------------
    # Sequences
    # ----------------------------------------

    def add_to_sequence(self, sequence_id: str, leads: list[dict]) -> dict:
        """
        Add up to 250 leads to a sequence at once.

        Each lead: {"email": "...", "first_name": "...", "last_name": "...", ...}
        """
        if len(leads) > 250:
            raise ValueError("Max 250 leads per sequence add call")
        return self._post(f"/sequences/{sequence_id}/leads", {"leads": leads})

    def add_to_sequence_bulk(self, sequence_id: str, leads: list[dict], delay: float = 2.5):
        """Add any number of leads to a sequence in 250-lead chunks."""
        total = 0
        for i in range(0, len(leads), 250):
            chunk = leads[i:i + 250]
            self.add_to_sequence(sequence_id, chunk)
            total += len(chunk)
            log.info(f"  Added {total}/{len(leads)} leads to sequence {sequence_id}")
            time.sleep(delay)  # 30/min limit = 2s between calls

    # ----------------------------------------
    # Email Validation (bulk)
    # ----------------------------------------

    def validate_emails(self, emails: list[str]) -> dict:
        """Validate up to 100,000 emails. Returns validation results."""
        if len(emails) > 100_000:
            raise ValueError("Max 100,000 emails per validation request")
        return self._post("/email-validations", {"emails": emails})
