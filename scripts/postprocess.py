#!/usr/bin/env python3
"""
Post-processor: clean existing data + generate CSV.gz downloads.
Run this ONCE to fix data quality and add CSV files without re-scraping.

Usage:
  python scripts/postprocess.py
"""

import csv
import gzip
import json
import sys
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def load_gz(path):
    with gzip.open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))


def save_gz(path, data):
    raw = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with gzip.open(path, "wb", compresslevel=6) as f:
        f.write(raw)
    print(f"  📦 {path.name}: {len(data):,} records → {path.stat().st_size/(1024*1024):.1f} MB")


def save_csv_gz(path, records):
    if not records:
        return
    headers = list(records[0].keys())
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6, newline="") as gz:
        writer = csv.DictWriter(gz, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        writer.writerows(records)
    print(f"  📦 {path.name}: {len(records):,} rows → {path.stat().st_size/(1024*1024):.1f} MB")


def main():
    print("═══ Post-processing SAP data ═══\n")

    # ── 1. Clean fields: remove .INCLUDE / .APPEND rows ──────────────────
    fields_gz = DATA_DIR / "sap_fields.json.gz"
    if fields_gz.exists():
        print("▶ Cleaning sap_fields.json.gz …")
        fields = load_gz(fields_gz)
        before = len(fields)
        fields = [
            f for f in fields
            if not f.get("field_name", "").startswith(".INCLU")
            and not f.get("field_name", "").startswith(".APPEND")
        ]
        removed = before - len(fields)
        print(f"  Removed {removed:,} structural rows (.INCLUDE/.APPEND)")
        print(f"  {before:,} → {len(fields):,} fields")
        save_gz(fields_gz, fields)
    else:
        print("⚠ sap_fields.json.gz not found, skipping clean")
        fields = []

    # ── 2. Load other datasets ────────────────────────────────────────────
    tables_gz = DATA_DIR / "sap_tables.json.gz"
    status_gz = DATA_DIR / "sap_status.json.gz"

    tables = load_gz(tables_gz) if tables_gz.exists() else []
    status = load_gz(status_gz) if status_gz.exists() else []

    print(f"\n  Tables: {len(tables):,}")
    print(f"  Fields: {len(fields):,}")
    print(f"  Status: {len(status):,}")

    # ── 3. Generate CSV.gz files ──────────────────────────────────────────
    print("\n▶ Generating CSV.gz files …")
    save_csv_gz(DATA_DIR / "sap_tables.csv.gz", tables)
    save_csv_gz(DATA_DIR / "sap_fields.csv.gz", fields)
    save_csv_gz(DATA_DIR / "sap_status.csv.gz", status)

    # ── 4. Update metadata.json ───────────────────────────────────────────
    print("\n▶ Updating metadata.json …")
    meta_path = DATA_DIR / "metadata.json"
    if meta_path.exists():
        with open(meta_path, "r") as f:
            meta = json.load(f)
    else:
        meta = {}

    # Update field count after cleaning
    meta.setdefault("summary", {})["total_fields_extracted"] = len(fields)

    # Add CSV.gz file entries
    meta.setdefault("files", {})
    meta["files"]["sap_tables.csv.gz"] = {"records": len(tables), "description": "SAP ABAP table catalog (CSV)"}
    meta["files"]["sap_fields.csv.gz"] = {"records": len(fields), "description": "Field structures per table (CSV)"}
    meta["files"]["sap_status.csv.gz"] = {"records": len(status), "description": "Scraping status per table (CSV)"}

    # Update fields count in json.gz entry too
    if "sap_fields.json.gz" in meta["files"]:
        meta["files"]["sap_fields.json.gz"]["records"] = len(fields)

    # Recalculate data type breakdown without INCLUDE/APPEND
    data_types = {}
    table_field_counts = {}
    for f in fields:
        dt = f.get("data_type", "Unknown") or "Unknown"
        data_types[dt] = data_types.get(dt, 0) + 1
        tn = f.get("table_name", "")
        table_field_counts[tn] = table_field_counts.get(tn, 0) + 1

    meta.setdefault("breakdowns", {})
    meta["breakdowns"]["by_data_type"] = dict(sorted(data_types.items(), key=lambda x: -x[1])[:25])
    top = sorted(table_field_counts.items(), key=lambda x: -x[1])[:20]
    meta["breakdowns"]["top_tables_by_fields"] = [{"table": t, "fields": c} for t, c in top]

    with open(meta_path, "w") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"  ✓ metadata.json updated")

    print(f"\n═══ Done! ═══")


if __name__ == "__main__":
    main()
