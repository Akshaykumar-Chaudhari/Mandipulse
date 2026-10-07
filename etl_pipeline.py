"""
MandiPulse ETL Pipeline (TAE 2)
================================
Ingests raw daily mandi commodity price data from the official data.gov.in
Agmarknet API and cleans it into a warehouse-ready table, per the plan
approved in TAE 1 (Section 6).

Two modes:
  --demo   Uses data/demo_raw_sample.csv, a SMALL SYNTHETIC FIXTURE built to
           contain the same categories of messiness as the real feed
           (duplicates, bad dates, naming variants, a missing modal price,
           a max<min anomaly). This lets the pipeline run end-to-end
           without live internet access, e.g. inside a sandboxed environment.
           IT IS NOT REAL GOVERNMENT DATA.
  --live   Calls the real data.gov.in REST API. Requires network access and
           a free API key (register instantly at https://data.gov.in ->
           "My Account" -> "API Access"). This is what must be used for the
           actual TAE 2 submission.

Usage:
    python etl_pipeline.py --demo
    python etl_pipeline.py --live --api-key YOUR_KEY --states Maharashtra "Madhya Pradesh" \
        --commodities Onion Tomato Wheat Potato Soybean --limit 5000 --pages 5
"""

import argparse
import os
import re
import sys
from datetime import datetime

import pandas as pd
import requests

API_BASE = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"

RAW_COLUMNS = [
    "state", "district", "market", "commodity", "variety",
    "grade", "arrival_date", "min_price", "max_price", "modal_price",
]


# --------------------------------------------------------------------------
# 1. INGESTION
# --------------------------------------------------------------------------

def fetch_live(api_key, states=None, commodities=None, limit=5000, pages=1):
    """Pull raw records from the real data.gov.in Agmarknet API, paginated.

    This is the real ingestion path for the TAE 2 submission. It performs
    no cleaning at all -- it returns exactly what the government feed
    reports, which is the point: the raw mess is the input to Section 2.
    """
    combos = [(s, c) for s in (states or [None]) for c in (commodities or [None])]
    all_records = []
    for s, c in combos:
        for page in range(pages):
            params = {"api-key": api_key, "format": "json",
                      "limit": limit, "offset": page * limit}
            if s:
                params["filters[state]"] = s
            if c:
                params["filters[commodity]"] = c
            resp = requests.get(API_BASE, params=params, timeout=30)
            resp.raise_for_status()
            batch = resp.json().get("records", [])
            if not batch:
                break
            all_records.extend(batch)
            if len(batch) < limit:
                break

    if not all_records:
        raise RuntimeError("No records returned -- check API key / filters.")

    df = pd.DataFrame(all_records)
    df.columns = [col.lower() for col in df.columns]
    return df.reindex(columns=RAW_COLUMNS)


def fetch_demo(path="data/demo_raw_sample.csv"):
    """Load the small synthetic fixture used to test the pipeline offline.

    SYNTHETIC DATA -- for demonstrating that the cleaning logic below works
    correctly. Not a substitute for the --live pull in the real submission.
    """
    return pd.read_csv(path, dtype=str)


# --------------------------------------------------------------------------
# 2. CLEANING / TRANSFORMATION  (implements TAE 1, Section 6.2)
# --------------------------------------------------------------------------

def parse_date(value):
    """Standardise Arrival_Date to ISO (YYYY-MM-DD).

    Raw feed issue: dates arrive as DD/MM/YYYY or DD-MM-YYYY strings.
    """
    if pd.isna(value) or str(value).strip() == "":
        return None
    value = str(value).strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None  # unparseable date -> flagged as missing, not guessed


def standardise_name(value):
    """Collapse casing/spacing variants of a commodity/variety name.

    Raw feed issue: "Onion" vs "onion" vs "ONION" vs "Onion(Nasik)".
    This keeps parenthetical detail out of the canonical key so it can
    still be used for grouping, while the original text is preserved
    separately for audit purposes.
    """
    if pd.isna(value):
        return None
    base = re.sub(r"\(.*?\)", "", str(value))  # drop "(Nasik)" etc.
    base = re.sub(r"\s+", " ", base).strip().lower()
    return base.title()


def clean(df_raw):
    """Apply the full TAE 1 Section 6.2 cleaning plan to a raw dataframe.

    Returns (clean_df, report) where report is a dict of counts describing
    what was found/fixed -- this is the evidence that the raw data really
    needed cleaning, for use in the TAE 2 write-up.
    """
    df = df_raw.copy()
    n_raw = len(df)

    for col in ["min_price", "max_price", "modal_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- issue: inconsistent date formats -> ISO, unparseable -> null ---
    df["arrival_date_iso"] = df["arrival_date"].apply(parse_date)
    n_bad_dates = df["arrival_date_iso"].isna().sum()
    df = df.dropna(subset=["arrival_date_iso"])  # can't place in Dim_Date

    # --- issue: inconsistent commodity/variety naming ---
    df["commodity_std"] = df["commodity"].apply(standardise_name)
    df["variety_std"] = df["variety"].apply(standardise_name)

    # --- issue: no natural primary key -> duplicate report rows ---
    dedup_keys = ["state", "district", "market", "commodity_std",
                  "variety_std", "arrival_date_iso"]
    n_before_dedup = len(df)
    df = df.drop_duplicates(subset=dedup_keys, keep="first")
    n_duplicates = n_before_dedup - len(df)

    # --- issue: data-entry anomaly, max_price < min_price ---
    anomaly_mask = df["max_price"] < df["min_price"]
    n_anomalies = int(anomaly_mask.sum())
    df["is_anomaly"] = anomaly_mask
    df.loc[anomaly_mask, ["min_price", "max_price", "modal_price"]] = None

    # --- issue: missing modal_price on low-arrival days ---
    missing_modal_mask = df["modal_price"].isna() & ~anomaly_mask
    n_missing_modal = int(missing_modal_mask.sum())
    df["is_estimated"] = missing_modal_mask
    df.loc[missing_modal_mask, "modal_price"] = (
        (df.loc[missing_modal_mask, "min_price"] + df.loc[missing_modal_mask, "max_price"]) / 2
    )

    # rows still unusable (anomaly with no valid replacement) are dropped
    # from KPI-ready output, but kept in a separate rejects frame for audit
    rejects = df[df["modal_price"].isna()]
    df = df[df["modal_price"].notna()]

    report = {
        "rows_raw": n_raw,
        "rows_bad_date_dropped": int(n_bad_dates),
        "rows_duplicate_removed": int(n_duplicates),
        "rows_flagged_anomaly": n_anomalies,
        "rows_modal_price_estimated": n_missing_modal,
        "rows_rejected_unusable": len(rejects),
        "rows_clean_output": len(df),
    }
    return df.reset_index(drop=True), report


# --------------------------------------------------------------------------
# 3. CLI
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="MandiPulse ETL pipeline")
    ap.add_argument("--demo", action="store_true", help="use the synthetic offline fixture")
    ap.add_argument("--live", action="store_true", help="call the real data.gov.in API")
    ap.add_argument("--input", default=None, help="path to a local raw CSV (e.g. a manual download)")
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--states", nargs="*", default=None)
    ap.add_argument("--commodities", nargs="*", default=None)
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--out", default="out/clean_prices.csv")
    ap.add_argument("--append-raw", action="store_true", help="append this pull to an existing --raw-out file instead of overwriting")
    ap.add_argument("--raw-out", default=None, help="also dump the raw pull (pre-clean) to this CSV, e.g. for the dashboard app's upload")
    args = ap.parse_args()

    if args.live:
        if not args.api_key:
            sys.exit("ERROR: --live requires --api-key (register free at data.gov.in)")
        raw = fetch_live(args.api_key, args.states, args.commodities, args.limit, args.pages)
        print(f"Pulled {len(raw)} raw rows from the LIVE data.gov.in API.")
    elif args.input:
        raw = pd.read_csv(args.input, dtype=str)
        raw.columns = [c.strip().lower().replace(" ", "_") for c in raw.columns]
        missing = [c for c in RAW_COLUMNS if c not in raw.columns]
        if missing:
            sys.exit(f"ERROR: input file is missing expected columns: {missing}. Found: {list(raw.columns)}")
        raw = raw[RAW_COLUMNS]
        print(f"Loaded {len(raw)} rows from {args.input}.")
    else:
        raw = fetch_demo()
        print(f"Loaded {len(raw)} rows from the SYNTHETIC demo fixture (not real data).")

    if args.raw_out:
        raw_all = raw
        if args.append_raw and os.path.exists(args.raw_out):
            prev = pd.read_csv(args.raw_out, dtype="string")
            raw_all = pd.concat([prev, raw.astype("string")], ignore_index=True).drop_duplicates()
        raw_all.to_csv(args.raw_out, index=False)
        print(f"Raw (pre-clean) pull written to {args.raw_out} -- upload this to the dashboard app.")

    clean_df, report = clean(raw)

    print("\nCleaning report:")
    for k, v in report.items():
        print(f"  {k}: {v}")

    clean_df.to_csv(args.out, index=False)
    print(f"\nClean output written to {args.out}")


if __name__ == "__main__":
    main()
