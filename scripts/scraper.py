#!/usr/bin/env python3
"""
SAP Data Dictionary Scraper — GitHub Actions Edition (v3 — Parallel + JSONL)
=============================================================================
Key improvements over v2:
  - JSONL append-only saves: never rewrites the full file, just appends new lines
  - Gzip compression: final output is .json.gz (~30 MB vs 422 MB raw)
  - GitHub-safe: all output files < 100 MB (GitHub's push limit)
  - Proper resume: reads existing JSONL on restart, skips already-done tables
  - Parallel async with semaphore-controlled concurrency

Save strategy:
  During scraping → append to .jsonl (instant, no re-serialization)
  On completion/shutdown → compress to .json.gz (one-time operation)
  Both files committed to git; dashboard reads .json.gz

Usage:
  python scripts/scraper.py [--max-tables N] [--workers 8] [--delay 0.3]
"""

import argparse
import asyncio
import gzip
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

RE_WHITESPACE = re.compile(r"\s+")
RE_INDEX_PAGE = re.compile(r"index-[a-z0-9]+(?:-\d+)?\.html")

SOURCE_NAME = "sapdatasheet"
BASE_URL = "https://www.sapdatasheet.org"
INDEX_URL = f"{BASE_URL}/abap/tabl/"


# ─── URL helpers ──────────────────────────────────────────────────────────────

def _resolve(base: str, href: str) -> str:
    return urljoin(base, href).split("#")[0]

def _url_path(url: str) -> str:
    after = url.split("://", 1)[-1]
    parts = after.split("/", 1)
    if len(parts) < 2:
        return "/"
    return ("/" + parts[1].split("?")[0].split("#")[0]).lower()


# ─── JSONL I/O (append-only, resumable) ──────────────────────────────────────

class JsonlWriter:
    """
    Append-only JSONL writer. Each call to append() writes lines and flushes.
    No re-serialization, no rewrite. O(new_records) per save, not O(total).
    """

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._count = 0
        # Count existing lines for resume
        if self.path.exists():
            with open(self.path, "r", encoding="utf-8") as f:
                self._count = sum(1 for line in f if line.strip())

    @property
    def count(self) -> int:
        return self._count

    def append(self, records: list):
        """Append records as JSONL lines. Flushes immediately."""
        if not records:
            return
        with open(self.path, "a", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self._count += len(records)

    def read_all(self) -> list:
        """Read all records from JSONL file."""
        if not self.path.exists():
            return []
        records = []
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def read_key_set(self, key: str) -> set:
        """Read a set of values for a specific key (for resume checking)."""
        result = set()
        if not self.path.exists():
            return result
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    val = obj.get(key)
                    if val:
                        result.add(val)
                except json.JSONDecodeError:
                    continue
        return result

    def compress_to_gz(self, gz_path: Path = None) -> Path:
        """Compress JSONL to .json.gz (as JSON array). Returns gz path."""
        if gz_path is None:
            gz_path = self.path.with_suffix(".json.gz")

        records = self.read_all()
        json_bytes = json.dumps(records, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

        with gzip.open(gz_path, "wb", compresslevel=6) as f:
            f.write(json_bytes)

        raw_mb = len(json_bytes) / (1024 * 1024)
        gz_mb = gz_path.stat().st_size / (1024 * 1024)
        ratio = gz_mb / raw_mb * 100 if raw_mb > 0 else 0
        print(f"  📦 {gz_path.name}: {raw_mb:.1f} MB → {gz_mb:.1f} MB ({ratio:.0f}%)")
        return gz_path


# ─── Simple JSON I/O ─────────────────────────────────────────────────────────

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
    tmp.replace(path)
    print(f"  💾 {path.name} ({path.stat().st_size / (1024*1024):.2f} MB)")

def save_json_gz(path: Path, data: Any) -> None:
    """Save data as gzipped JSON array."""
    path.parent.mkdir(parents=True, exist_ok=True)
    json_bytes = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with gzip.open(path, "wb", compresslevel=6) as f:
        f.write(json_bytes)
    raw_mb = len(json_bytes) / (1024 * 1024)
    gz_mb = path.stat().st_size / (1024 * 1024)
    print(f"  📦 {path.name}: {raw_mb:.1f} MB → {gz_mb:.1f} MB")

def save_json_pretty(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Parsing functions ───────────────────────────────────────────────────────

def _parse_index_page(html: str, page_url: str, seen_tables: Set[str]):
    soup = BeautifulSoup(html, "lxml")
    new_indexes = []
    new_tables = []
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for a in soup.find_all("a", href=True):
        abs_url = _resolve(page_url, a["href"])
        path = _url_path(abs_url)
        fname = path.rsplit("/", 1)[-1]
        if path.startswith("/abap/tabl/") and RE_INDEX_PAGE.fullmatch(fname):
            new_indexes.append(abs_url)

    for tr in soup.find_all("tr"):
        row_texts = []
        for cell in tr.find_all(["td", "th"]):
            txt = cell.get_text(strip=True)
            if txt:
                row_texts.append(RE_WHITESPACE.sub(" ", cell.get_text(" ", strip=True)))

        table_link = None
        for a in tr.find_all("a", href=True):
            text = RE_WHITESPACE.sub(" ", a.get_text(" ", strip=True)).upper()
            if not text:
                continue
            abs_url = _resolve(page_url, a["href"])
            path = _url_path(abs_url)
            fname = path.rsplit("/", 1)[-1]
            if path.startswith("/abap/tabl/") and fname.endswith(".html") and not fname.startswith("index-"):
                table_link = (text, abs_url)
                break

        if not table_link:
            continue
        table_name, table_url = table_link
        if table_name in seen_tables:
            continue
        seen_tables.add(table_name)

        description = table_category = delivery_class = ""
        upper_texts = [x.upper() for x in row_texts]
        if table_name in upper_texts:
            idx = upper_texts.index(table_name)
            after = row_texts[idx + 1:]
            description = after[0] if len(after) > 0 else ""
            table_category = after[1] if len(after) > 1 else ""
            delivery_class = after[2] if len(after) > 2 else ""

        new_tables.append({
            "source": SOURCE_NAME, "table_name": table_name, "table_url": table_url,
            "description": description, "table_category": table_category,
            "delivery_class": delivery_class, "discovered_at_utc": now,
        })

    return new_indexes, new_tables


def _parse_fields(html: str, table_name: str, table_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
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
            "source": SOURCE_NAME, "table_name": table_name, "table_url": table_url,
            "position": position, "field_name": field_name,
            "data_element": de, "domain": dom, "data_type": dt,
            "length": ln, "decimals": dec, "description": desc,
            "check_table": chk, "scraped_at_utc": now,
        })

    rows.sort(key=lambda r: int(r["position"]) if r["position"].isdigit() else 99999)
    return rows


# ─── Async Scraper ────────────────────────────────────────────────────────────

class AsyncSAPScraper:

    def __init__(self, max_workers=8, delay_seconds=0.3, max_retries=4, timeout_seconds=30):
        self.max_workers = max_workers
        self.delay = delay_seconds
        self.max_retries = max_retries
        self.timeout = timeout_seconds
        self.request_count = 0
        self.error_count = 0
        self._sem = None
        self._client = None
        self._shutdown = False

    async def start(self):
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
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
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

    async def fetch(self, url: str) -> Optional[str]:
        if self._shutdown:
            return None
        async with self._sem:
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
                    self.error_count += 1
                    if attempt < self.max_retries:
                        await asyncio.sleep(self.delay * (2 ** attempt))
            return None

    # ── Phase 1: Index discovery ──────────────────────────────────────────

    async def discover_tables(self) -> List[Dict[str, str]]:
        pending = deque([INDEX_URL])
        visited: Set[str] = set()
        seen: Set[str] = set()
        all_tables = []

        while pending and not self._shutdown:
            wave = []
            while pending and len(wave) < self.max_workers * 2:
                url = pending.popleft()
                if url not in visited:
                    visited.add(url)
                    wave.append(url)
            if not wave:
                break

            htmls = await asyncio.gather(*[self.fetch(u) for u in wave], return_exceptions=True)

            for page_url, html in zip(wave, htmls):
                if isinstance(html, Exception) or html is None:
                    continue
                new_idx, new_tbl = _parse_index_page(html, page_url, seen)
                all_tables.extend(new_tbl)
                for u in new_idx:
                    if u not in visited:
                        pending.append(u)

            if len(visited) % 10 == 0:
                print(f"  Index: {len(visited)} pages | {len(all_tables)} tables | {len(pending)} queued | {self.request_count} reqs")

        all_tables.sort(key=lambda t: t["table_name"])
        return all_tables

    # ── Phase 2: Field extraction ─────────────────────────────────────────

    async def extract_fields(
        self,
        tables: List[Dict],
        fields_writer: JsonlWriter,
        status_writer: JsonlWriter,
        batch_size: int = 200,
    ):
        """Extract fields with JSONL append-only saves."""
        total = len(tables)

        for start in range(0, total, batch_size):
            if self._shutdown:
                break

            chunk = tables[start : start + batch_size]
            tasks = [self._process_one(t) for t in chunk]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            chunk_fields = []
            chunk_status = []
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")

            for table, result in zip(chunk, results):
                if isinstance(result, Exception) or result is None:
                    chunk_status.append({
                        "source": SOURCE_NAME, "table_name": table["table_name"],
                        "table_url": table["table_url"], "status": "error",
                        "field_count": 0,
                        "error": str(result)[:300] if result else "Fetch failed",
                        "scraped_at_utc": now,
                    })
                else:
                    fields, status = result
                    chunk_fields.extend(fields)
                    chunk_status.append(status)

            # JSONL append — instant, no rewrite
            fields_writer.append(chunk_fields)
            status_writer.append(chunk_status)

            done = min(start + batch_size, total)
            ok = sum(1 for s in chunk_status if s["status"] == "ok")
            print(
                f"  [{done}/{total}] "
                f"+{len(chunk_fields)} fields | "
                f"{ok}/{len(chunk)} ok | "
                f"total: {fields_writer.count} fields | "
                f"{self.request_count} reqs"
            )

    async def _process_one(self, table: Dict):
        html = await self.fetch(table["table_url"])
        if html is None:
            return None
        fields = _parse_fields(html, table["table_name"], table["table_url"])
        status = {
            "source": SOURCE_NAME, "table_name": table["table_name"],
            "table_url": table["table_url"],
            "status": "ok" if fields else "empty",
            "field_count": len(fields), "error": None,
            "scraped_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        return fields, status


# ─── Main ─────────────────────────────────────────────────────────────────────

async def async_main(args):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    tables_path = DATA_DIR / "sap_tables.json"
    fields_jsonl = DATA_DIR / "sap_fields.jsonl"
    status_jsonl = DATA_DIR / "sap_status.jsonl"
    fields_gz = DATA_DIR / "sap_fields.json.gz"
    status_gz = DATA_DIR / "sap_status.json.gz"
    tables_gz = DATA_DIR / "sap_tables.json.gz"
    meta_path = DATA_DIR / "metadata.json"

    run_start = datetime.now(timezone.utc)

    print("═" * 60)
    print("  SAP Data Dictionary Scraper v3 (Parallel + JSONL + Gzip)")
    print("═" * 60)
    print(f"  Started:  {run_start.isoformat(timespec='seconds')}")
    print(f"  Workers:  {args.workers}")
    print(f"  Delay:    {args.delay}s/worker")
    print(f"  Data dir: {DATA_DIR}")
    print()

    scraper = AsyncSAPScraper(max_workers=args.workers, delay_seconds=args.delay)

    # Graceful shutdown
    def on_sigterm(signum, frame):
        print("\n⚠ SIGTERM — shutting down, will compress and commit …")
        scraper.shutdown()
    signal.signal(signal.SIGTERM, on_sigterm)

    await scraper.start()

    # ── Phase 1: Discover ─────────────────────────────────────────────────
    print("▶ PHASE 1: Discovering tables …")
    t0 = time.monotonic()

    existing_tables = load_json(tables_path) or []
    existing_names = {t["table_name"] for t in existing_tables}

    try:
        discovered = await scraper.discover_tables()
        new_tables = [t for t in discovered if t["table_name"] not in existing_names]
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("  ⚠ Discovery interrupted")
        new_tables = []

    merged = {t["table_name"]: t for t in existing_tables}
    for t in new_tables:
        merged[t["table_name"]] = t
    all_tables = sorted(merged.values(), key=lambda t: t["table_name"])

    save_json(tables_path, all_tables)
    print(f"  ✅ Catalog: {len(all_tables)} tables ({len(new_tables)} new) in {time.monotonic()-t0:.0f}s\n")

    if args.skip_fields:
        _finalize(meta_path, tables_path, tables_gz, fields_jsonl, fields_gz,
                  status_jsonl, status_gz, run_start, all_tables)
        await scraper.close()
        return

    # ── Phase 2: Fields (JSONL append) ────────────────────────────────────
    print("▶ PHASE 2: Extracting fields …")
    t0 = time.monotonic()

    # Restore JSONL from .json.gz if needed (for resume between runs)
    for jsonl_path, gz_path in [(fields_jsonl, fields_gz), (status_jsonl, status_gz)]:
        if not jsonl_path.exists() and gz_path.exists():
            print(f"  Restoring {jsonl_path.name} from {gz_path.name} …")
            with gzip.open(gz_path, "rb") as f:
                records = json.loads(f.read().decode("utf-8"))
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            print(f"    Restored {len(records)} records")

    fields_writer = JsonlWriter(fields_jsonl)
    status_writer = JsonlWriter(status_jsonl)

    # Resume: find already-done tables from JSONL
    already_done = status_writer.read_key_set("table_name")
    # Only count ok/empty as done (not errors — retry those)
    if already_done:
        status_records = status_writer.read_all()
        already_done = {s["table_name"] for s in status_records if s.get("status") in ("ok", "empty")}

    pending = [t for t in all_tables if t["table_name"] not in already_done]
    if args.max_tables > 0:
        pending = pending[:args.max_tables]

    print(f"  Already done: {len(already_done)}")
    print(f"  Pending:      {len(pending)}")
    print(f"  Resume from:  {fields_writer.count} fields in JSONL")
    print()

    try:
        await scraper.extract_fields(
            pending, fields_writer, status_writer,
            batch_size=max(args.workers * 8, 200),
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("  ⚠ Interrupted — JSONL data is safe on disk")

    elapsed = time.monotonic() - t0
    print(f"  ✅ Extraction: {elapsed:.0f}s | {fields_writer.count} total fields\n")

    # ── Phase 3: Compress + metadata ──────────────────────────────────────
    _finalize(meta_path, tables_path, tables_gz, fields_jsonl, fields_gz,
              status_jsonl, status_gz, run_start, all_tables)

    await scraper.close()
    print(f"\n═══ Done! {scraper.request_count} requests, {scraper.error_count} errors ═══")


def _finalize(meta_path, tables_path, tables_gz, fields_jsonl, fields_gz,
              status_jsonl, status_gz, run_start, all_tables):
    """Compress JSONL → .json.gz and write metadata."""
    print("▶ PHASE 3: Compressing + metadata …")

    # Compress tables
    save_json_gz(tables_gz, all_tables)

    # Compress fields JSONL → .json.gz
    fields_writer = JsonlWriter(fields_jsonl)
    fields_writer.compress_to_gz(fields_gz)
    fields_count = fields_writer.count

    # Compress status JSONL → .json.gz
    status_writer = JsonlWriter(status_jsonl)
    status_records = status_writer.read_all()
    # Deduplicate status: keep latest entry per table_name
    status_dedup = {}
    for s in status_records:
        status_dedup[s["table_name"]] = s
    status_final = sorted(status_dedup.values(), key=lambda s: s["table_name"])
    save_json_gz(status_gz, status_final)

    # Read fields for metadata stats (scan JSONL without loading all into memory)
    data_types: Dict[str, int] = {}
    table_field_counts: Dict[str, int] = {}
    with open(fields_jsonl, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                dt = obj.get("data_type", "Unknown") or "Unknown"
                data_types[dt] = data_types.get(dt, 0) + 1
                tn = obj.get("table_name", "")
                table_field_counts[tn] = table_field_counts.get(tn, 0) + 1
            except json.JSONDecodeError:
                continue

    ok = sum(1 for s in status_final if s["status"] == "ok")
    empty = sum(1 for s in status_final if s["status"] == "empty")
    errors = sum(1 for s in status_final if s["status"] == "error")
    total = len(all_tables)
    coverage = (ok + empty) / total * 100 if total else 0

    cats = {}
    for t in all_tables:
        c = t.get("table_category", "Unknown") or "Unknown"
        cats[c] = cats.get(c, 0) + 1

    top = sorted(table_field_counts.items(), key=lambda x: -x[1])[:20]

    run_end = datetime.now(timezone.utc)
    metadata = {
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
            "total_fields_extracted": fields_count,
            "coverage_percent": round(coverage, 2),
        },
        "breakdowns": {
            "by_category": dict(sorted(cats.items(), key=lambda x: -x[1])),
            "by_data_type": dict(sorted(data_types.items(), key=lambda x: -x[1])[:25]),
            "top_tables_by_fields": [{"table": t, "fields": c} for t, c in top],
        },
        "files": {
            "sap_tables.json.gz": {"records": total, "description": "SAP ABAP table catalog"},
            "sap_fields.json.gz": {"records": fields_count, "description": "Field structures per table"},
            "sap_status.json.gz": {"records": len(status_final), "description": "Scraping status per table"},
        },
    }

    save_json_pretty(meta_path, metadata)
    print(f"\n  📊 {total} tables | {fields_count} fields | {coverage:.1f}% coverage")


def main():
    parser = argparse.ArgumentParser(description="SAP Data Dictionary Scraper v3")
    parser.add_argument("--max-tables", type=int, default=0)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--delay", type=float, default=0.3)
    parser.add_argument("--skip-fields", action="store_true")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
