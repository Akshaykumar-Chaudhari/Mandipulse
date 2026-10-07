"""
Loads the cleaned ETL output (out/clean_prices.csv) into the star-schema
SQLite warehouse defined in sql/schema.sql, generating surrogate keys for
each dimension as it goes.

Usage:
    python load_warehouse.py --input out/clean_prices.csv --db out/mandipulse.db
"""

import argparse
import sqlite3
from datetime import date

import pandas as pd


def get_or_create(cur, table, unique_cols, values, id_col):
    where = " AND ".join(f"{c} = ?" for c in unique_cols)
    cur.execute(f"SELECT {id_col} FROM {table} WHERE {where}", values)
    row = cur.fetchone()
    if row:
        return row[0]
    cols = ", ".join(unique_cols)
    placeholders = ", ".join("?" for _ in unique_cols)
    cur.execute(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", values)
    return cur.lastrowid


def load(input_csv, db_path, schema_sql):
    df = pd.read_csv(input_csv)
    # NULL never equals NULL in SQL lookups, so blank keys would create a new
    # dimension row every time. Give blanks an explicit value first.
    for col in ["grade", "district"]:
        df[col] = df[col].fillna("Unknown").replace("", "Unknown")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executescript(open(schema_sql).read())

    for _, r in df.iterrows():
        y, m, d = map(int, r["arrival_date_iso"].split("-"))
        weekday = date(y, m, d).weekday()
        date_id = get_or_create(
            cur, "Dim_Date", ["full_date", "day", "month", "quarter", "year", "is_weekend"],
            [r["arrival_date_iso"], d, m, (m - 1) // 3 + 1, y, int(weekday >= 5)], "date_id",
        )
        market_id = get_or_create(
            cur, "Dim_Market", ["market_name", "district", "state"],
            [r["market"], r["district"], r["state"]], "market_id",
        )
        commodity_id = get_or_create(
            cur, "Dim_Commodity", ["commodity_name"], [r["commodity_std"]], "commodity_id",
        )
        variety_id = get_or_create(
            cur, "Dim_Variety", ["variety_name", "grade"],
            [r["variety_std"], r.get("grade")], "variety_id",
        )
        cur.execute(
            """INSERT INTO Fact_MandiPrice
               (date_id, market_id, commodity_id, variety_id,
                min_price, max_price, modal_price, is_estimated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (date_id, market_id, commodity_id, variety_id,
             r["min_price"], r["max_price"], r["modal_price"], int(r["is_estimated"])),
        )

    conn.commit()

    counts = {}
    for t in ["Dim_Date", "Dim_Market", "Dim_Commodity", "Dim_Variety", "Fact_MandiPrice"]:
        counts[t] = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    conn.close()
    return counts


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="out/clean_prices.csv")
    ap.add_argument("--db", default="out/mandipulse.db")
    ap.add_argument("--schema", default="sql/schema.sql")
    args = ap.parse_args()

    counts = load(args.input, args.db, args.schema)
    print(f"Loaded warehouse at {args.db}:")
    for t, c in counts.items():
        print(f"  {t}: {c} rows")
