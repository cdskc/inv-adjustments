import glob
import os
from datetime import date
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# Column name constants
COL_FACILITY = "Facility Facility ID"
COL_DATE = "@Inventory Adjustment Adjustment Date"
COL_PRODUCT = "Product PRD Name"
COL_QUANTITY = "@Inventory Adjustment Adjustment Quantity"

GROUP_COLS = [COL_FACILITY, COL_DATE, COL_PRODUCT]

# Friendlier column headers for the HTML report
DISPLAY_HEADERS = {
    "Facility Facility ID": "Facility",
    "@Inventory Adjustment Adjustment Date": "Date",
    "Product PRD Name": "Product",
    "Drug Drug Ndc Hyphenated": "NDC",
    "Drug DEA Drug Schedule ": "DEA Sched",
    "Sys User User ID": "User",
    "Inv Adjustment Reason Description": "Reason",
    "@Inventory Adjustment Adjustment Quantity": "Qty",
    "'@Inventory Adjustment Reference Number": "Reference #",
}


def load_latest_csv(data_dir: str = "data") -> pd.DataFrame:
    """Load the most recently modified CSV from the data directory."""
    files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in '{data_dir}'")
    latest = max(files, key=os.path.getmtime)
    print(f"Loading: {latest}")
    df = pd.read_csv(latest, dtype=str)
    # Quantity may contain commas (e.g. "1,000") — strip them and convert to int
    df[COL_QUANTITY] = (
        df[COL_QUANTITY].str.replace(",", "", regex=False).astype(int)
    )
    return df


def remove_offsetting_adjustments(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Drop any group (Facility + Date + Product) whose quantities sum to zero.

    These represent paired adjustments that cancel each other out — e.g. moving
    inventory from one NDC to another — and are not considered unusual activity.

    Returns (flagged, removed) DataFrames.
    """
    group_sums = df.groupby(GROUP_COLS)[COL_QUANTITY].transform("sum")
    flagged = df[group_sums != 0].copy()
    removed = df[group_sums == 0].copy()

    removed_groups = removed.drop_duplicates(subset=GROUP_COLS)[GROUP_COLS]
    print(f"\nRemoved {len(removed_groups)} offsetting group(s) "
          f"({len(removed)} row(s) total).")

    return flagged, removed


def write_html_report(
    flagged: pd.DataFrame,
    removed: pd.DataFrame,
    output_dir: str = "output",
) -> str:
    """Write a styled HTML report of flagged adjustments to output_dir."""
    os.makedirs(output_dir, exist_ok=True)
    report_date = date.today().strftime("%Y_%m_%d")
    out_path = os.path.join(output_dir, f"flagged_adjustments_{report_date}.html")

    display_df = flagged.rename(columns=DISPLAY_HEADERS)

    # Stripe rows by facility+date+product group for readability
    group_keys = [DISPLAY_HEADERS[c] for c in GROUP_COLS]
    group_ids = display_df.groupby(group_keys, sort=False).ngroup()
    row_classes = group_ids.map(lambda i: "row-even" if i % 2 == 0 else "row-odd")

    rows_html = ""
    for (_, row), cls in zip(display_df.iterrows(), row_classes):
        qty = row["Qty"]
        qty_style = " style='color:#c0392b;font-weight:bold'" if qty < 0 else " style='color:#27ae60;font-weight:bold'"
        cells = "".join(
            f"<td{qty_style if col == 'Qty' else ''}>{'' if pd.isna(v) else v}</td>"
            for col, v in row.items()
        )
        rows_html += f"<tr class='{cls}'>{cells}</tr>\n"

    headers_html = "".join(f"<th>{h}</th>" for h in display_df.columns)

    removed_count = removed.drop_duplicates(subset=GROUP_COLS).shape[0]
    generated_on = date.today().strftime("%B %d, %Y")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Flagged CS Inventory Adjustments – {generated_on}</title>
  <style>
    body {{ font-family: Arial, sans-serif; font-size: 13px; margin: 24px; color: #222; }}
    h1 {{ font-size: 20px; margin-bottom: 4px; }}
    .meta {{ color: #555; margin-bottom: 18px; font-size: 12px; }}
    .summary {{ display: flex; gap: 24px; margin-bottom: 20px; }}
    .card {{ background: #f4f6f8; border-radius: 6px; padding: 12px 20px; min-width: 140px; }}
    .card .num {{ font-size: 28px; font-weight: bold; color: #2c3e50; }}
    .card .label {{ font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: .5px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th {{ background: #2c3e50; color: #fff; padding: 8px 10px; text-align: left;
          font-size: 12px; position: sticky; top: 0; }}
    td {{ padding: 6px 10px; border-bottom: 1px solid #e0e0e0; white-space: nowrap; }}
    tr.row-even {{ background: #ffffff; }}
    tr.row-odd  {{ background: #f0f4f8; }}
    tr:hover td {{ background: #fff9c4; }}
  </style>
</head>
<body>
  <h1>Controlled Substance Inventory Adjustments – Review Required</h1>
  <div class="meta">Generated {generated_on} &nbsp;|&nbsp; Offsetting (zero-sum) groups removed automatically</div>
  <div class="summary">
    <div class="card"><div class="num">{len(flagged)}</div><div class="label">Rows flagged</div></div>
    <div class="card"><div class="num">{removed_count}</div><div class="label">Groups removed</div></div>
    <div class="card"><div class="num">{len(removed)}</div><div class="label">Rows removed</div></div>
  </div>
  <table>
    <thead><tr>{headers_html}</tr></thead>
    <tbody>
{rows_html}    </tbody>
  </table>
</body>
</html>
"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    return out_path


def main():
    df = load_latest_csv()

    print(f"\nTotal rows loaded: {len(df)}")

    flagged, removed = remove_offsetting_adjustments(df)

    print(f"Rows remaining after filter: {len(flagged)}")

    out_path = write_html_report(flagged, removed)
    print(f"\nReport written to: {out_path}")


if __name__ == "__main__":
    main()