import glob
import os
from datetime import date
from io import BytesIO

import pandas as pd

# Column name constants
COL_FACILITY = "Facility Facility ID"
COL_DATE = "@Inventory Adjustment Adjustment Date"
COL_PRODUCT = "Product PRD Name"
COL_QUANTITY = "@Inventory Adjustment Adjustment Quantity"
COL_REASON = "Inv Adjustment Reason Description"

GROUP_COLS = [COL_FACILITY, COL_PRODUCT]

# Reasons that get sorted to the bottom of the report
DEFERRED_REASONS = {"EXPIRED", "Return To Supplier"}

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

# Looker uses different (friendly) column names than the raw system export.
# Map Looker names → canonical names so the rest of the pipeline works.
LOOKER_COL_MAP = {
    "Store": "Facility Facility ID",
    "Adj Date": "@Inventory Adjustment Adjustment Date",
    "Product Name": "Product PRD Name",
    "NDC": "Drug Drug Ndc Hyphenated",
    "Schedule": "Drug DEA Drug Schedule ",
    "User ID": "Sys User User ID",
    "Reason": "Inv Adjustment Reason Description",
    "Qty": "@Inventory Adjustment Adjustment Quantity",
    "Reference Number": "'@Inventory Adjustment Reference Number",
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """If the CSV has Looker-style column names, rename to canonical names."""
    if "Store" in df.columns and COL_FACILITY not in df.columns:
        df = df.rename(columns=LOOKER_COL_MAP)
    return df


def _parse_csv_df(df: pd.DataFrame) -> pd.DataFrame:
    """Apply common transforms after reading a CSV into a DataFrame."""
    df = _normalize_columns(df)
    df[COL_QUANTITY] = (
        df[COL_QUANTITY].str.replace(",", "", regex=False).astype(int)
    )
    return df


def load_latest_csv(data_dir: str = "data") -> pd.DataFrame:
    """Load the most recently modified CSV from the data directory."""
    files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in '{data_dir}'")
    latest = max(files, key=os.path.getmtime)
    print(f"Loading: {latest}")
    df = pd.read_csv(latest, dtype=str)
    return _parse_csv_df(df)


def load_csv_from_bytes(csv_bytes: bytes) -> pd.DataFrame:
    """Parse CSV from raw bytes (as received from a webhook POST)."""
    df = pd.read_csv(BytesIO(csv_bytes), dtype=str)
    return _parse_csv_df(df)


def _find_zero_sum_indices(quantities: list[int]) -> list[int]:
    """Return local indices of the largest subset of *quantities* that sums to zero.

    Uses bitmask enumeration — feasible because real-world groups rarely exceed
    ~10 rows.  Falls back to a whole-group sum check for groups larger than 24.
    """
    n = len(quantities)
    if n == 0:
        return []

    # Fast path: entire group balances
    if sum(quantities) == 0:
        return list(range(n))

    # Bitmask search for the largest zero-sum subset
    if n > 24:  # safety cap; 2^24 ≈ 16M — still fast, but unlikely in practice
        return []

    best_mask = 0
    best_count = 0
    for mask in range(1, 1 << n):
        s = 0
        cnt = 0
        for i in range(n):
            if mask >> i & 1:
                s += quantities[i]
                cnt += 1
        if s == 0 and cnt > best_count:
            best_count = cnt
            best_mask = mask

    return [i for i in range(n) if best_mask >> i & 1]


def remove_offsetting_adjustments(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Remove the largest zero-sum subset within each Facility + Product group.

    Rather than dropping an entire group only when its total is zero, this
    function identifies the biggest subset of rows whose quantities perfectly
    cancel.  Unmatched rows survive into the flagged report.

    Returns (flagged, removed) DataFrames.
    """
    remove_idx: list[int] = []

    for _, grp in df.groupby(GROUP_COLS, sort=False):
        quantities = grp[COL_QUANTITY].tolist()
        zero_positions = _find_zero_sum_indices(quantities)
        if zero_positions:
            idx_list = grp.index.tolist()
            remove_idx.extend(idx_list[p] for p in zero_positions)

    removed = df.loc[df.index.isin(remove_idx)]
    flagged = df.loc[~df.index.isin(remove_idx)]

    print(
        f"\nRemoved {len(removed)} offsetting row(s) "
        f"across {removed.groupby(GROUP_COLS, sort=False).ngroups if not removed.empty else 0} group(s)."
    )
    print(f"Rows remaining after filter: {len(flagged)}")

    return flagged, removed


def generate_html_report(
    flagged: pd.DataFrame,
    removed: pd.DataFrame,
) -> str:
    """Build a styled HTML report of flagged adjustments and return as a string."""
    reason_col = DISPLAY_HEADERS[COL_REASON]

    # Sort so deferred reasons (Expired, Return To Supplier) appear at the bottom
    is_deferred = flagged[COL_REASON].isin(DEFERRED_REASONS)
    sorted_df = pd.concat([flagged[~is_deferred], flagged[is_deferred]])
    display_df = sorted_df.rename(columns=DISPLAY_HEADERS)

    # Stripe rows by facility+product group for readability
    group_keys = [DISPLAY_HEADERS[c] for c in GROUP_COLS]
    group_ids = display_df.groupby(group_keys, sort=False).ngroup()
    row_classes = group_ids.map(lambda i: "row-even" if i % 2 == 0 else "row-odd")

    # Inline styles for email compatibility (Gmail strips <style> blocks)
    td_base = "padding:6px 10px;border-bottom:1px solid #e0e0e0;white-space:nowrap"
    row_bg = {"row-even": "#ffffff", "row-odd": "#f0f4f8"}

    rows_html = ""
    deferred_sep_added = False
    num_cols = len(display_df.columns)
    for (_, row), cls in zip(display_df.iterrows(), row_classes):
        # Insert a separator row before the first deferred-reason row
        if not deferred_sep_added and row[reason_col] in DEFERRED_REASONS:
            deferred_sep_added = True
            rows_html += (
                f"<tr><td colspan=\"{num_cols}\" style=\"padding:10px;background:#eee;"
                f"font-weight:bold;font-size:12px;color:#555;border-bottom:2px solid #ccc\">"
                f"Expired / Return to Supplier</td></tr>\n"
            )
        qty = row["Qty"]
        bg = row_bg[cls]
        cells = ""
        for col, v in row.items():
            val = "" if pd.isna(v) else v
            if col == "Qty":
                color = "#c0392b" if qty < 0 else "#27ae60"
                cells += f"<td style=\"{td_base};background:{bg};color:{color};font-weight:bold\">{val}</td>"
            else:
                cells += f"<td style=\"{td_base};background:{bg}\">{val}</td>"
        rows_html += f"<tr>{cells}</tr>\n"

    th_style = "background:#2c3e50;color:#fff;padding:8px 10px;text-align:left;font-size:12px"
    headers_html = "".join(f"<th style=\"{th_style}\">{h}</th>" for h in display_df.columns)

    removed_count = removed.drop_duplicates(subset=GROUP_COLS).shape[0]
    generated_on = date.today().strftime("%B %d, %Y")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Flagged CS Inventory Adjustments – {generated_on}</title>
</head>
<body style="font-family:Arial,sans-serif;font-size:13px;margin:24px;color:#222">
  <h1 style="font-size:20px;margin-bottom:4px">Controlled Substance Inventory Adjustments – Review Required</h1>
  <div style="color:#555;margin-bottom:18px;font-size:12px">Generated {generated_on} &nbsp;|&nbsp; Offsetting (zero-sum) groups removed automatically</div>
  <table cellpadding="0" cellspacing="0" style="margin-bottom:20px">
    <tr>
      <td style="background:#f4f6f8;border-radius:6px;padding:12px 20px;min-width:140px">
        <div style="font-size:28px;font-weight:bold;color:#2c3e50">{len(flagged)}</div>
        <div style="font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.5px">Rows flagged</div>
      </td>
      <td style="width:24px"></td>
      <td style="background:#f4f6f8;border-radius:6px;padding:12px 20px;min-width:140px">
        <div style="font-size:28px;font-weight:bold;color:#2c3e50">{removed_count}</div>
        <div style="font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.5px">Groups removed</div>
      </td>
      <td style="width:24px"></td>
      <td style="background:#f4f6f8;border-radius:6px;padding:12px 20px;min-width:140px">
        <div style="font-size:28px;font-weight:bold;color:#2c3e50">{len(removed)}</div>
        <div style="font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.5px">Rows removed</div>
      </td>
    </tr>
  </table>
  <table cellpadding="0" cellspacing="0" style="border-collapse:collapse;width:100%">
    <thead><tr>{headers_html}</tr></thead>
    <tbody>
{rows_html}    </tbody>
  </table>
</body>
</html>
"""
    return html


def write_html_report(
    flagged: pd.DataFrame,
    removed: pd.DataFrame,
    output_dir: str = "output",
) -> str:
    """Write a styled HTML report of flagged adjustments to output_dir."""
    os.makedirs(output_dir, exist_ok=True)
    report_date = date.today().strftime("%Y_%m_%d")
    out_path = os.path.join(output_dir, f"flagged_adjustments_{report_date}.html")

    html = generate_html_report(flagged, removed)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    return out_path


def process_csv(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Run the full processing pipeline: filter + generate HTML report.

    Returns (flagged, removed, html_content).
    """
    flagged, removed = remove_offsetting_adjustments(df)
    html_content = generate_html_report(flagged, removed)
    return flagged, removed, html_content
