#!/usr/bin/env python3
"""
SAP Data Dictionary Scraper — GitHub Actions Edition
=====================================================
Standalone scraper adapted from nb_komatsu_bambu_scraping_sap_table.
Extracts ABAP table catalog and field structures from sapdatasheet.org,
saves results as JSON files in the data/ directory.

Features:
  - Dynamic discovery of all index pages (CLUSTER, POOL, SLASH, A–W)
  - Exponential-backoff retries for HTTP errors
  - Resumable: compares against existing data to skip already-scraped tables
  - Writes compact JSON for GitHub-friendly storage
  - Generates metadata.json with extraction stats and timestamps

Usage:
  python scripts/scraper.py [--max-tables N] [--delay SECONDS] [--skip-fields]
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

# ─── Constants ────────────────────────────────────────────────────────────────

ABAP_TYPES = {
    "ACCP", "CHAR", "CLNT", "CUKY", "CURR", "DATS", "DEC",
    "DF16_DEC", "DF16_RAW", "DF34_DEC", "DF34_RAW",
    "FLTP", "INT1", "INT2", "INT4", "INT8", "LANG", "LCHR", "LRAW",
    "NUMC", "PREC", "QUAN", "RAW", "RAWSTRING", "SSTRING", "STRING",
    "TIMS", "UNIT", "VARC",
}

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ─── Scraper Class ────────────────────────────────────────────────────────────

class SAPDataDictScraper:
    """
    Reusable scraper for sapdatasheet.org ABAP table dictionary.
    Discovers index pages dynamically, extracts table catalog and field structures.
    """

    def __init__(
        self,
        base_url: str = "https://www.sapdatasheet.org",
        source: str = "sapdatasheet",
        delay_seconds: float = 1.5,
        max_retries: int = 4,
        timeout_seconds: int = 40,
    ):
        self.base_url = base_url.rstrip("/")
        self.source = source
        self.index_url = f"{self.base_url}/abap/tabl/"
        self.delay_seconds = delay_seconds
        self.max_retries = max_retries
        self.client = httpx.Client(
            timeout=timeout_seconds,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )
        self._request_count = 0

    # ── HTTP helper ───────────────────────────────────────────────────────

    def _get(self, url: str) -> str:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.client.get(url)
                resp.raise_for_status()
                self._request_count += 1
                time.sleep(self.delay_seconds)
                return resp.text
            except Exception as exc:
                last_error = exc
                wait = self.delay_seconds * (2 ** (attempt - 1))
                print(
                    f"  [{attempt}/{self.max_retries}] Error fetching {url}: {exc}. "
                    f"Retrying in {wait:.1f}s …"
                )
                time.sleep(wait)
        raise RuntimeError(f"Failed to fetch {url}: {last_error}")

    # ── Step 1: Discover table catalog ────────────────────────────────────

    def discover_tables(self) -> Iterable[Dict[str, str]]:
        """Yield table metadata dicts by crawling all index pages."""
        pending_pages = [self.index_url]
        visited_pages: set = set()
        seen_tables: set = set()

        while pending_pages:
            page_url = pending_pages.pop(0)
            if page_url in visited_pages:
                continue

            try:
                html = self._get(page_url)
            except RuntimeError as e:
                print(f"  ⚠ Skipping page {page_url}: {e}")
                continue

            visited_pages.add(page_url)
            soup = BeautifulSoup(html, "html.parser")

            # Discover sub-index pages
            for a in soup.find_all("a", href=True):
                abs_url = urljoin(page_url, a["href"]).split("#")[0]
                path = urlparse(abs_url).path.lower()
                file_name = path.rsplit("/", 1)[-1]
                is_index = (
                    path.startswith("/abap/tabl/")
                    and re.fullmatch(r"index-[a-z0-9]+(?:-\d+)?\.html", file_name)
                )
                if is_index and abs_url not in visited_pages and abs_url not in pending_pages:
                    pending_pages.append(abs_url)

            # Extract table rows
            for tr in soup.find_all("tr"):
                row_texts = [
                    re.sub(r"\s+", " ", cell.get_text(" ", strip=True))
                    for cell in tr.find_all(["td", "th"])
                    if cell.get_text(strip=True)
                ]

                table_link = None
                for a in tr.find_all("a", href=True):
                    text = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).upper()
                    abs_url = urljoin(page_url, a["href"]).split("#")[0]
                    path = urlparse(abs_url).path.lower()
                    file_name = path.rsplit("/", 1)[-1]
                    is_table_page = (
                        text
                        and path.startswith("/abap/tabl/")
                        and file_name.endswith(".html")
                        and not file_name.startswith("index-")
                    )
                    if is_table_page:
                        table_link = (text, abs_url)
                        break

                if not table_link:
                    continue

                table_name, table_url = table_link
                if table_name in seen_tables:
                    continue
                seen_tables.add(table_name)

                description = ""
                table_category = ""
                delivery_class = ""
                upper_texts = [x.upper() for x in row_texts]
                if table_name in upper_texts:
                    idx = upper_texts.index(table_name)
                    after = row_texts[idx + 1:]
                    description = after[0] if len(after) > 0 else ""
                    table_category = after[1] if len(after) > 1 else ""
                    delivery_class = after[2] if len(after) > 2 else ""

                yield {
                    "source": self.source,
                    "table_name": table_name,
                    "table_url": table_url,
                    "description": description,
                    "table_category": table_category,
                    "delivery_class": delivery_class,
                    "discovered_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                }

            if len(visited_pages) % 5 == 0:
                print(
                    f"  Index progress: {len(visited_pages)} pages visited, "
                    f"{len(seen_tables)} tables found, "
                    f"{len(pending_pages)} pages queued"
                )

    # ── Step 2: Extract field structure ───────────────────────────────────

    def table_fields(self, table_name: str, table_url: str) -> List[Dict[str, str]]:
        """Extract the field/column structure for a single SAP table."""
        html = self._get(table_url)
        soup = BeautifulSoup(html, "html.parser")
        rows: List[Dict[str, str]] = []

        for tr in soup.find_all("tr"):
            cells = [
                re.sub(r"\s+", " ", td.get_text(" ", strip=True))
                for td in tr.find_all("td")
                if td.get_text(strip=True)
            ]
            if not cells or not cells[0].isdigit() or len(cells) < 2:
                continue

            position = cells[0]
            values = cells[1:]
            field_name = values[0]

            type_idx = next(
                (
                    idx
                    for idx, v in enumerate(values)
                    if v.upper() in ABAP_TYPES and idx > 0
                ),
                None,
            )

            data_element = ""
            domain = ""
            data_type = ""
            length = ""
            decimals = ""
            description = ""
            check_table = ""

            if type_idx is not None:
                data_type = values[type_idx].upper()
                data_element = values[1] if type_idx >= 2 else ""
                domain = values[type_idx - 1] if type_idx >= 1 else ""
                length = values[type_idx + 1] if len(values) > type_idx + 1 else ""
                decimals = values[type_idx + 2] if len(values) > type_idx + 2 else ""
                description = values[type_idx + 3] if len(values) > type_idx + 3 else ""
                check_table = values[type_idx + 4] if len(values) > type_idx + 4 else ""
            else:
                description = " ".join(values[1:])

            rows.append({
                "source": self.source,
                "table_name": table_name,
                "table_url": table_url,
                "position": position,
                "field_name": field_name,
                "data_element": data_element,
                "domain": domain,
                "data_type": data_type,
                "length": length,
                "decimals": decimals,
                "description": description,
                "check_table": check_table,
                "scraped_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            })

        rows.sort(key=lambda r: int(r["position"]) if r["position"].isdigit() else 99999)
        return rows

    def close(self):
        self.client.close()


# ─── File I/O helpers ─────────────────────────────────────────────────────────

def load_json(path: Path) -> Any:
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=None, separators=(",", ":"))
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"  ✓ Saved {path.name} ({size_mb:.2f} MB)")


def save_json_pretty(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Main pipeline ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SAP Data Dictionary Scraper")
    parser.add_argument("--max-tables", type=int, default=0, help="Limit tables to scrape fields for (0 = all)")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between requests in seconds")
    parser.add_argument("--skip-fields", action="store_true", help="Only discover tables, skip field extraction")
    parser.add_argument("--resume", action="store_true", default=True, help="Resume from existing data (default: True)")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    tables_path = DATA_DIR / "sap_tables.json"
    fields_path = DATA_DIR / "sap_fields.json"
    status_path = DATA_DIR / "sap_status.json"
    meta_path = DATA_DIR / "metadata.json"

    run_start = datetime.now(timezone.utc)
    print(f"═══ SAP Data Dictionary Scraper ═══")
    print(f"  Started: {run_start.isoformat(timespec='seconds')}")
    print(f"  Data dir: {DATA_DIR}")
    print()

    scraper = SAPDataDictScraper(delay_seconds=args.delay, max_retries=4)

    # ── Phase 1: Discover tables ──────────────────────────────────────────
    print("▶ Phase 1: Discovering table catalog …")
    existing_tables = load_json(tables_path) or []
    existing_table_names = {t["table_name"] for t in existing_tables}

    new_tables = []
    try:
        for table_dict in scraper.discover_tables():
            if table_dict["table_name"] not in existing_table_names:
                new_tables.append(table_dict)
                existing_table_names.add(table_dict["table_name"])
    except KeyboardInterrupt:
        print("\n  ⚠ Discovery interrupted — saving partial results")

    all_tables = existing_tables + new_tables
    all_tables.sort(key=lambda t: t["table_name"])
    save_json(tables_path, all_tables)
    print(f"  Tables discovered: {len(all_tables)} total ({len(new_tables)} new)")
    print()

    if args.skip_fields:
        print("  --skip-fields active, skipping field extraction.")
        _write_metadata(meta_path, run_start, all_tables, [], [])
        scraper.close()
        return

    # ── Phase 2: Extract fields ───────────────────────────────────────────
    print("▶ Phase 2: Extracting field structures …")
    existing_fields = load_json(fields_path) or []
    existing_status = load_json(status_path) or []

    already_done = {s["table_name"] for s in existing_status if s["status"] in ("ok", "empty")}
    pending = [t for t in all_tables if t["table_name"] not in already_done]

    if args.max_tables > 0:
        pending = pending[: args.max_tables]

    print(f"  Already processed: {len(already_done)}")
    print(f"  Pending: {len(pending)}")
    print()

    new_fields = []
    new_status = []
    batch_count = 0
    SAVE_EVERY = 50

    try:
        for i, table in enumerate(pending, start=1):
            table_name = table["table_name"]
            table_url = table["table_url"]

            try:
                fields = scraper.table_fields(table_name, table_url)
                new_fields.extend(fields)
                new_status.append({
                    "source": table.get("source", "sapdatasheet"),
                    "table_name": table_name,
                    "table_url": table_url,
                    "status": "ok" if fields else "empty",
                    "field_count": len(fields),
                    "error": None,
                    "scraped_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                })
            except Exception as exc:
                new_status.append({
                    "source": table.get("source", "sapdatasheet"),
                    "table_name": table_name,
                    "table_url": table_url,
                    "status": "error",
                    "field_count": 0,
                    "error": str(exc)[:500],
                    "scraped_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                })

            if i % 10 == 0 or i == len(pending):
                print(f"  [{i}/{len(pending)}] Last: {table_name} — Fields batch: {len(new_fields)}")

            # Periodic save
            batch_count += 1
            if batch_count >= SAVE_EVERY or i == len(pending):
                all_fields = existing_fields + new_fields
                all_status = existing_status + new_status
                save_json(fields_path, all_fields)
                save_json(status_path, all_status)
                existing_fields = all_fields
                existing_status = all_status
                new_fields = []
                new_status = []
                batch_count = 0

    except KeyboardInterrupt:
        print("\n  ⚠ Field extraction interrupted — saving partial results")
        if new_fields or new_status:
            all_fields = existing_fields + new_fields
            all_status = existing_status + new_status
            save_json(fields_path, all_fields)
            save_json(status_path, all_status)

    final_fields = load_json(fields_path) or []
    final_status = load_json(status_path) or []

    # ── Phase 3: Write metadata ───────────────────────────────────────────
    _write_metadata(meta_path, run_start, all_tables, final_fields, final_status)
    scraper.close()

    print()
    print(f"═══ Done! Total HTTP requests: {scraper._request_count} ═══")


def _write_metadata(
    meta_path: Path,
    run_start: datetime,
    tables: list,
    fields: list,
    status: list,
) -> None:
    run_end = datetime.now(timezone.utc)

    ok_count = sum(1 for s in status if s["status"] == "ok")
    empty_count = sum(1 for s in status if s["status"] == "empty")
    error_count = sum(1 for s in status if s["status"] == "error")
    total_tables = len(tables)
    coverage = (ok_count + empty_count) / total_tables * 100 if total_tables else 0

    # Category breakdown
    categories: Dict[str, int] = {}
    for t in tables:
        cat = t.get("table_category", "Unknown") or "Unknown"
        categories[cat] = categories.get(cat, 0) + 1

    # Data type breakdown
    data_types: Dict[str, int] = {}
    for f in fields:
        dt = f.get("data_type", "Unknown") or "Unknown"
        data_types[dt] = data_types.get(dt, 0) + 1

    # Top tables by field count
    table_field_counts: Dict[str, int] = {}
    for f in fields:
        tn = f.get("table_name", "")
        table_field_counts[tn] = table_field_counts.get(tn, 0) + 1
    top_tables = sorted(table_field_counts.items(), key=lambda x: -x[1])[:20]

    metadata = {
        "last_run": {
            "started_at_utc": run_start.isoformat(timespec="seconds"),
            "finished_at_utc": run_end.isoformat(timespec="seconds"),
            "duration_seconds": int((run_end - run_start).total_seconds()),
        },
        "summary": {
            "total_tables_discovered": total_tables,
            "tables_with_fields": ok_count,
            "tables_empty": empty_count,
            "tables_with_errors": error_count,
            "total_fields_extracted": len(fields),
            "coverage_percent": round(coverage, 2),
        },
        "breakdowns": {
            "by_category": dict(sorted(categories.items(), key=lambda x: -x[1])),
            "by_data_type": dict(sorted(data_types.items(), key=lambda x: -x[1])[:25]),
            "top_tables_by_fields": [{"table": t, "fields": c} for t, c in top_tables],
        },
        "files": {
            "sap_tables.json": {
                "records": total_tables,
                "description": "SAP ABAP table catalog (name, description, category, delivery class)",
            },
            "sap_fields.json": {
                "records": len(fields),
                "description": "Field/column structures for each SAP table",
            },
            "sap_status.json": {
                "records": len(status),
                "description": "Scraping status per table (ok/empty/error)",
            },
        },
    }

    save_json_pretty(meta_path, metadata)
    print(f"\n  📊 Summary:")
    print(f"     Tables: {total_tables}")
    print(f"     Fields: {len(fields)}")
    print(f"     Coverage: {coverage:.1f}%")
    print(f"     Duration: {metadata['last_run']['duration_seconds']}s")


if __name__ == "__main__":
    main()
