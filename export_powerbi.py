"""Export the star-schema warehouse to an Excel workbook (one sheet per table) for Power BI.
Usage: python export_powerbi.py --db out/mandipulse.db --out out/MandiPulse_PowerBI_Import.xlsx
"""
import argparse, sqlite3
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--db", default="out/mandipulse.db")
ap.add_argument("--out", default="out/MandiPulse_PowerBI_Import.xlsx")
a = ap.parse_args()

conn = sqlite3.connect(a.db)
with pd.ExcelWriter(a.out, engine="openpyxl") as w:
    for t in ["Dim_Date", "Dim_Market", "Dim_Commodity", "Dim_Variety", "Fact_MandiPrice"]:
        df = pd.read_sql(f"SELECT * FROM {t}", conn)
        df.to_excel(w, sheet_name=t, index=False)
        print(t, len(df), "rows")
conn.close()
print("Written", a.out)
