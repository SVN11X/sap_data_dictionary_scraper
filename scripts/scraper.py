#!/usr/bin/env python3
"""
SAP Data Dictionary Scraper — GitHub Actions Edition (Parallel)
================================================================
High-performance async scraper for sapdatasheet.org ABAP table dictionary.

Optimizations vs. sequential version:
  - Async HTTP (httpx.AsyncClient) with multiple requests in flight
  - Semaphore-controlled concurrency (default: 8 workers)
  - Connection pooling with keep-alive
  - lxml parser (~5x faster than html.parser)
  - Parallel index page discovery (wave-based async BFS)
  - Parallel field extraction (asyncio.gather per batch)
  - Adaptive delay: per-worker throttle, not global blocking sleep
  - Precompiled regex patterns
  - Incremental saves every batch (safe on interruption)

Estimated speedup: ~10-20x vs sequential version

Usage:
  python scripts/scraper.py [--max-tables N] [--workers 8] [--delay 0.3]
"""

import argparse
import asyncio
import json
import os
import re
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

# ─── Constants ────────────────────────────────────────────────────────────────

ABAP_TYPES: frozenset = frozenset({
    "ACCP", "CHAR", "CLNT", "CUKY", "CURR", "DATS", "DEC",
    "DF16_DEC", "DF16_RAW", "DF34_DEC", "DF34_RAW",
    "FLTP", "INT1", "INT2", "INT4", "INT8", "LANG", "LCHR", "LRAW",
    "NUMC", "PREC", "QUAN", "RAW", "RAWSTRING", "SSTRING", "STRING",
    "TIMS", "UNIT", "VARC",
})

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Precompiled regex
RE_WHITESPACE = re.compile(r"\s+")
RE_INDEX_PAGE = re.compile(r"index-[a-z0-9]+(?:-\d+)?\.html")

SOURCE_NAME = "sapdatasheet"
BASE_URL = "https://www.sapdatasheet.org"
INDEX_URL = f"{BASE_URL}/abap/tabl/"


# ─── Fast URL helpers ─────────────────────────────────────────────────────────

def _resolve(base: str, href: str) -> str:
    """Resolve relative URL and strip fragment."""
    return urljoin(base, href).split("#")[0]


def _url_path(url: str) -> str:
    """Extract lowercase path from URL — faster than urlparse for hot loop."""
    after = url.split("://", 1)[-1]
    parts = after.split("/", 1)
    if len(parts) < 2:
        return "/"
    return ("/" + parts[1].split("?")[0].split("#")[0]).lower()


# ─── Pure parsing functions (no I/O) ─────────────────────────────────────────

def _parse_index_page(html: str, page_url: str, seen_tables: Set[str]):
    """
    Parse one index page. Returns (new_index_urls, new_table_dicts).
    Pure function — no network calls.
    """
    soup = BeautifulSoup(html, "lxml")
    new_indexes: List[str] = []
    new_tables: List[Dict[str, str]] = []
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Collect sub-index links
    for a in soup.find_all("a", href=True):
        abs_url = _resolve(page_url, a["href"])
        path = _url_path(abs_url)
        fname = path.rsplit("/", 1)[-1]
        if path.startswith("/abap/tabl/") and RE_INDEX_PAGE.fullmatch(fname):
            new_indexes.append(abs_url)

    # Collect table rows
    for tr in soup.find_all("tr"):
        row_texts = []
        for cell in tr.find_all(["td", "th"]):
            txt = cell.get_text(strip=True)
            if txt:
                row_texts.append(RE_WHITESPACE.sub(" ", cell.get_text(" ", strip=True)))

        # Find the table link in this row
        table_link = None
        for a in tr.find_all("a", href=True):
            text = RE_WHITESPACE.sub(" ", a.get_text(" ", strip=True)).upper()
            if not text:
                continue
            abs_url = _resolve(page_url, a["href"])
            path = _url_path(abs_url)
            fname = path.rsplit("/", 1)[-1]
            if (
                path.startswith("/abap/tabl/")
                and fname.endswith(".html")
                and not fname.startswith("index-")
            ):
                table_link = (text, abs_url)
                break

        if not table_link:
            continue

        table_name, table_url = table_link
        if table_name in seen_tables:
            continue
        seen_tables.add(table_name)

        # Extract metadata from adjacent cells
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

        new_tables.append({
            "source": SOURCE_NAME,
            "table_name": table_name,
            "table_url": table_url,
            "description": description,
            "table_category": table_category,
            "delivery_class": delivery_class,
            "discovered_at_utc": now,
        })

    return new_indexes, new_tables


def _parse_fields(html: str, table_name: str, table_url: str) -> List[Dict[str, str]]:
    """Parse field structure from a table detail page."""
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict[str, str]] = []
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for tr in soup.find_all("tr"):
        cells = [
            RE_WHITESPACE.sub(" ", td.get_text(" ", strip=True))
            for td in tr.find_all("td")
            if td.get_text(strip=True)
        ]
        if not cells or not cells[0].isdigit() or len(cells) < 2:
            continue

        position = cells[0]
        values = cells[1:]
        field_name = values[0]

        type_idx = next(
            (i for i, v in enumerate(values) if v.upper() in ABAP_TYPES and i > 0),
            None,
        )

        de = dom = dt = ln = dec = desc = chk = ""

        if type_idx is not None:
            dt = values[type_idx].upper()
            de = values[1] if type_idx >= 2 else ""
            dom = values[type_idx - 1] if type_idx >= 1 else ""
            ln = values[type_idx + 1] if len(values) > type_idx + 1 else ""
            dec = values[type_idx + 2] if len(values) > type_idx + 2 else ""
            desc = values[type_idx + 3] if len(values) > type_idx + 3 else ""
            chk = values[type_idx + 4] if len(values) > type_idx + 4 else ""
        else:
            desc = " ".join(values[1:])

        rows.append({
            "source": SOURCE_NAME,
            "table_name": table_name,
            "table_url": table_url,
            "position": position,
            "field_name": field_name,
            "data_element": de,
            "domain": dom,
            "data_type": dt,
            "length": ln,
            "decimals": dec,
            "description": desc,
            "check_table": chk,
            "scraped_at_utc": now,
        })

    rows.sort(key=lambda r: int(r["position"]) if r["position"].isdigit() else 99999)
    return rows


# ─── Async Scraper Engine ─────────────────────────────────────────────────────

class AsyncSAPScraper:
    """Parallel async scraper with semaphore-controlled concurrency."""

    def __init__(
        self,
        max_workers: int = 8,
        delay_seconds: float = 0.3,
        max_retries: int = 4,
        timeout_seconds: int = 30,
    ):
        self.max_workers = max_workers
        self.delay = delay_seconds
        self.max_retries = max_retries
        self.timeout = timeout_seconds
        self.request_count = 0
        self.error_count = 0
        self._sem: Optional[asyncio.Semaphore] = None
        self._client: Optional[httpx.AsyncClient] = None
        self._shutdown = False

    async def start(self):
        """Initialize async client and semaphore."""
        self._sem = asyncio.Semaphore(self.max_workers)
        self._client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=self.max_workers + 4,
                max_keepalive_connections=self.max_workers,
                keepalive_expiry=30,
            ),
            timeout=self.timeout,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
            },
        )

    async def close(self):
        if self._client:
            await self._client.aclose()

    def shutdown(self):
        self._shutdown = True

    # ── Core HTTP fetch with concurrency control ──────────────────────────

    async def fetch(self, url: str) -> Optional[str]:
        """Fetch URL with semaphore, retries, per-worker delay."""
        if self._shutdown:
            return None
        async with self._sem:
            last_err = None
            for attempt in range(1, self.max_retries + 1):
                if self._shutdown:
                    return None
                try:
                    resp = await self._client.get(url)
                    resp.raise_for_status()
                    self.request_count += 1
                    await asyncio.sleep(self.delay)
                    return resp.text
                except Exception as e:
                    last_err = e
                    self.error_count += 1
                    if attempt < self.max_retries:
                        await asyncio.sleep(self.delay * (2 ** attempt))
            # All retries failed
            print(f"  ✗ Failed ({self.max_retries} attempts): {url} — {last_err}")
            return None

    # ── Phase 1: Parallel index discovery (wave-based BFS) ────────────────

    async def discover_tables(self) -> List[Dict[str, str]]:
        """Crawl index pages with parallel BFS waves."""
        pending: deque = deque([INDEX_URL])
        visited: Set[str] = set()
        seen_tables: Set[str] = set()
        all_tables: List[Dict[str, str]] = []

        while pending and not self._shutdown:
            # Build next wave: up to max_workers URLs
            wave = []
            while pending and len(wave) < self.max_workers * 2:
                url = pending.popleft()
                if url not in visited:
                    visited.add(url)
                    wave.append(url)
            if not wave:
                break

            # Fetch all wave URLs concurrently
            htmls = await asyncio.gather(
                *[self.fetch(url) for url in wave],
                return_exceptions=True,
            )

            # Parse results (CPU-bound but fast with lxml)
            for page_url, html in zip(wave, htmls):
                if isinstance(html, Exception) or html is None:
                    continue
                new_indexes, new_tables = _parse_index_page(html, page_url, seen_tables)
                all_tables.extend(new_tables)
                for idx_url in new_indexes:
                    if idx_url not in visited:
                        pending.append(idx_url)

            print(
                f"  Index: {len(visited)} pages | "
                f"{len(all_tables)} tables | "
                f"{len(pending)} queued | "
                f"{self.request_count} reqs"
            )

        all_tables.sort(key=lambda t: t["table_name"])
        return all_tables

    # ── Phase 2: Parallel field extraction ────────────────────────────────

    async def extract_fields(
        self,
        tables: List[Dict],
        save_callback=None,
        batch_size: int = 64,
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Extract fields for all tables with controlled parallelism.
        save_callback(fields_chunk, status_chunk) called after each batch.
        """
        all_fields = []
        all_status = []
        total = len(tables)

        for start in range(0, total, batch_size):
            if self._shutdown:
                break

            chunk = tables[start : start + batch_size]

            # Fire all requests in this chunk concurrently
            # (semaphore inside fetch() controls actual parallelism)
            tasks = [self._process_one(t) for t in chunk]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            chunk_fields = []
            chunk_status = []
            for table, result in zip(chunk, results):
                if isinstance(result, Exception) or result is None:
                    chunk_status.append({
                        "source": SOURCE_NAME,
                        "table_name": table["table_name"],
                        "table_url": table["table_url"],
                        "status": "error",
                        "field_count": 0,
                        "error": str(result)[:500] if result else "Fetch failed",
                        "scraped_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    })
                else:
                    fields, status = result
                    chunk_fields.extend(fields)
                    chunk_status.append(status)

            all_fields.extend(chunk_fields)
            all_status.extend(chunk_status)

            done = min(start + batch_size, total)
            ok = sum(1 for s in chunk_status if s["status"] == "ok")
            print(
                f"  [{done}/{total}] "
                f"+{len(chunk_fields)} fields | "
                f"{ok}/{len(chunk)} ok | "
                f"{self.request_count} reqs"
            )

            if save_callback:
                save_callback(chunk_fields, chunk_status)

        return all_fields, all_status

    async def _process_one(self, table: Dict) -> Optional[Tuple[List[Dict], Dict]]:
        """Fetch + parse one table."""
        html = await self.fetch(table["table_url"])
        if html is None:
            return None
        fields = _parse_fields(html, table["table_name"], table["table_url"])
        status = {
            "source": SOURCE_NAME,
            "table_name": table["table_name"],
            "table_url": table["table_url"],
            "status": "ok" if fields else "empty",
            "field_count": len(fields),
            "error": None,
            "scraped_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        return fields, status


# ─── File I/O ─────────────────────────────────────────────────────────────────

def load_json(path: Path) -> Any:
    if path.exists() and path.stat().st_size > 2:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(path)  # atomic rename
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"  💾 {path.name} ({size_mb:.2f} MB)")


def save_json_pretty(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def async_main(args):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tables_path = DATA_DIR / "sap_tables.json"
    fields_path = DATA_DIR / "sap_fields.json"
    status_path = DATA_DIR / "sap_status.json"
    meta_path = DATA_DIR / "metadata.json"

    run_start = datetime.now(timezone.utc)

    print("═" * 60)
    print("  SAP Data Dictionary Scraper (Parallel Async)")
    print("═" * 60)
    print(f"  Started:  {run_start.isoformat(timespec='seconds')}")
    print(f"  Workers:  {args.workers} concurrent")
    print(f"  Delay:    {args.delay}s per worker")
    print(f"  Throughput: ~{args.workers / max(args.delay, 0.1):.0f} req/s theoretical max")
    print(f"  Data dir: {DATA_DIR}")
    print()

    scraper = AsyncSAPScraper(
        max_workers=args.workers,
        delay_seconds=args.delay,
        max_retries=4,
    )

    # Graceful shutdown on SIGTERM
    def on_sigterm(signum, frame):
        print("\n⚠ SIGTERM — initiating graceful shutdown …")
        scraper.shutdown()
    signal.signal(signal.SIGTERM, on_sigterm)

    await scraper.start()

    # ── Phase 1: Discover ─────────────────────────────────────────────────
    print("▶ PHASE 1: Discovering table catalog …")
    t0 = time.monotonic()

    existing_tables = load_json(tables_path) or []
    existing_names = {t["table_name"] for t in existing_tables}

    try:
        discovered = await scraper.discover_tables()
        new_tables = [t for t in discovered if t["table_name"] not in existing_names]
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("  ⚠ Discovery interrupted")
        new_tables = []
        discovered = []

    # Merge + deduplicate
    merged = {t["table_name"]: t for t in existing_tables}
    for t in new_tables:
        merged[t["table_name"]] = t
    all_tables = sorted(merged.values(), key=lambda t: t["table_name"])

    save_json(tables_path, all_tables)
    print(f"  ✅ Catalog: {len(all_tables)} tables ({len(new_tables)} new) in {time.monotonic()-t0:.0f}s\n")

    if args.skip_fields:
        _write_metadata(meta_path, run_start, all_tables, [], [])
        await scraper.close()
        return

    # ── Phase 2: Fields ───────────────────────────────────────────────────
    print("▶ PHASE 2: Extracting field structures …")
    t0 = time.monotonic()

    existing_fields = load_json(fields_path) or []
    existing_status = load_json(status_path) or []
    already_done = {s["table_name"] for s in existing_status if s["status"] in ("ok", "empty")}

    pending = [t for t in all_tables if t["table_name"] not in already_done]
    if args.max_tables > 0:
        pending = pending[:args.max_tables]

    print(f"  Done:    {len(already_done)}")
    print(f"  Pending: {len(pending)}")
    print()

    # Incremental saver
    acc_fields = list(existing_fields)
    acc_status = list(existing_status)

    def save_incremental(chunk_f, chunk_s):
        acc_fields.extend(chunk_f)
        acc_status.extend(chunk_s)
        save_json(fields_path, acc_fields)
        save_json(status_path, acc_status)

    try:
        await scraper.extract_fields(
            pending,
            save_callback=save_incremental,
            batch_size=max(args.workers * 6, 48),
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("  ⚠ Extraction interrupted — partial data saved")

    elapsed = time.monotonic() - t0
    print(f"  ✅ Field extraction done in {elapsed:.0f}s\n")

    # ── Phase 3: Metadata ─────────────────────────────────────────────────
    final_fields = load_json(fields_path) or acc_fields
    final_status = load_json(status_path) or acc_status
    _write_metadata(meta_path, run_start, all_tables, final_fields, final_status)

    await scraper.close()
    print()
    print(f"═══ Done! {scraper.request_count} requests, {scraper.error_count} errors ═══")


def _write_metadata(meta_path, run_start, tables, fields, status):
    run_end = datetime.now(timezone.utc)

    ok = sum(1 for s in status if s["status"] == "ok")
    empty = sum(1 for s in status if s["status"] == "empty")
    errors = sum(1 for s in status if s["status"] == "error")
    total = len(tables)
    coverage = (ok + empty) / total * 100 if total else 0

    cats = {}
    for t in tables:
        c = t.get("table_category", "Unknown") or "Unknown"
        cats[c] = cats.get(c, 0) + 1

    dtypes = {}
    for f in fields:
        d = f.get("data_type", "Unknown") or "Unknown"
        dtypes[d] = dtypes.get(d, 0) + 1

    tfc = {}
    for f in fields:
        n = f.get("table_name", "")
        tfc[n] = tfc.get(n, 0) + 1
    top = sorted(tfc.items(), key=lambda x: -x[1])[:20]

    meta = {
        "last_run": {
            "started_at_utc": run_start.isoformat(timespec="seconds"),
            "finished_at_utc": run_end.isoformat(timespec="seconds"),
            "duration_seconds": int((run_end - run_start).total_seconds()),
        },
        "summary": {
            "total_tables_discovered": total,
            "tables_with_fields": ok,
            "tables_empty": empty,
            "tables_with_errors": errors,
            "total_fields_extracted": len(fields),
            "coverage_percent": round(coverage, 2),
        },
        "breakdowns": {
            "by_category": dict(sorted(cats.items(), key=lambda x: -x[1])),
            "by_data_type": dict(sorted(dtypes.items(), key=lambda x: -x[1])[:25]),
            "top_tables_by_fields": [{"table": t, "fields": c} for t, c in top],
        },
        "files": {
            "sap_tables.json": {"records": total, "description": "SAP ABAP table catalog"},
            "sap_fields.json": {"records": len(fields), "description": "Field structures per table"},
            "sap_status.json": {"records": len(status), "description": "Scraping status per table"},
        },
    }

    save_json_pretty(meta_path, meta)
    print(f"  📊 Summary: {total} tables | {len(fields)} fields | {coverage:.1f}% coverage | {meta['last_run']['duration_seconds']}s")


def main():
    parser = argparse.ArgumentParser(description="SAP Data Dictionary Scraper (Parallel)")
    parser.add_argument("--max-tables", type=int, default=0, help="Max tables for field extraction (0=all)")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent HTTP workers (default: 8)")
    parser.add_argument("--delay", type=float, default=0.3, help="Per-worker delay in seconds (default: 0.3)")
    parser.add_argument("--skip-fields", action="store_true", help="Catalog only, skip field extraction")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
