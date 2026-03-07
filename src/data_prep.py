"""
data_prep.py — Enrich sales rows with per-line purchase cost.

Flow:
  1. Read the stock report (contains `code` → `purchasePrice`).
  2. Join onto the sales report via the `code` column.
  3. Compute   Purchase Price  =  purchasePrice_from_stock  ×  qty_from_sales
  4. Return the enriched sales DataFrame with a clean `Purchase Price` column
     ready for analysis.py to consume.

Column names are fully configurable via Config so they match whatever the user's
actual sheet headers are.
"""

import logging

import numpy as np
import pandas as pd

log = logging.getLogger("customer_analysis.data_prep")


def merge_purchase_prices(
    sales_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    col_sales_code: str,
    col_sales_qty: str,
    col_stock_code: str,
    col_stock_purchase_price: str,
) -> pd.DataFrame:
    """
    Enrich sales_df with a `Purchase Price` column derived from stock_df.

    Args:
        sales_df:                   Raw sales line-item DataFrame.
        stock_df:                   Raw stock/inventory DataFrame.
        col_sales_code:             Column name for item code in sales_df.
        col_sales_qty:              Column name for quantity in sales_df.
        col_stock_code:             Column name for item code in stock_df.
        col_stock_purchase_price:   Column name for unit purchase price in stock_df.

    Returns:
        A copy of sales_df with a `Purchase Price` column equal to
        unit_purchase_price × quantity  (0.0 when code not found in stock).
    """
    _validate_join_columns(
        sales_df, stock_df,
        col_sales_code, col_sales_qty,
        col_stock_code, col_stock_purchase_price,
    )

    # ── 1. Build a clean price lookup: {code → unit_purchase_price} ──────────
    price_lookup = _build_price_lookup(stock_df, col_stock_code, col_stock_purchase_price)
    log.info(
        "Stock price lookup built",
        extra={"unique_codes": len(price_lookup)},
    )

    # ── 2. Prepare sales columns ──────────────────────────────────────────────
    sales = sales_df.copy()

    sales["_code_clean"] = (
        sales[col_sales_code]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    sales["_qty"] = pd.to_numeric(sales[col_sales_qty], errors="coerce").fillna(0.0)

    # ── 3. Map unit price by code ─────────────────────────────────────────────
    sales["_unit_pp"] = sales["_code_clean"].map(price_lookup).fillna(0.0)

    # ── 4. Compute line-level purchase cost ───────────────────────────────────
    #  Purchase Price  =  unit_purchase_price  ×  quantity
    sales["Purchase Price"] = (sales["_unit_pp"] * sales["_qty"]).round(4)

    # ── 5. Diagnostics ───────────────────────────────────────────────────────
    unmatched_mask = (sales["_unit_pp"] == 0.0) & (sales["_code_clean"] != "nan")
    unmatched_codes = sales.loc[unmatched_mask, col_sales_code].unique()
    if len(unmatched_codes) > 0:
        log.warning(
            "Some sales codes were not found in the stock report — "
            "Purchase Price defaulted to 0 for these lines.",
            extra={
                "unmatched_count": int(unmatched_mask.sum()),
                "sample_codes": [str(c) for c in unmatched_codes[:10]],
            },
        )
    else:
        log.info("All sales codes matched successfully in stock report.")

    matched_pct = round(
        (1 - unmatched_mask.sum() / max(len(sales), 1)) * 100, 1
    )
    log.info(
        "Purchase price merge complete",
        extra={
            "sales_rows": len(sales),
            "match_pct": matched_pct,
            "total_purchase_cost": round(float(sales["Purchase Price"].sum()), 2),
        },
    )

    # ── 6. Drop temp columns ──────────────────────────────────────────────────
    sales.drop(columns=["_code_clean", "_qty", "_unit_pp"], inplace=True)

    return sales


def _build_price_lookup(
    stock_df: pd.DataFrame,
    col_code: str,
    col_price: str,
) -> dict[str, float]:
    """
    Build a {normalised_code: unit_price} dict from the stock DataFrame.

    Handles duplicate codes (same item, multiple stock entries) by taking the
    LAST occurrence — which represents the most-recently-entered purchase price,
    matching typical point-of-sale stock management behaviour.
    """
    stock = stock_df[[col_code, col_price]].copy()

    # Normalise numeric types that gspread may have read as strings.
    stock[col_price] = pd.to_numeric(stock[col_price], errors="coerce").fillna(0.0)

    # Normalise the code key.
    stock["_code_norm"] = stock[col_code].astype(str).str.strip().str.lower()

    # Drop rows with empty/invalid codes.
    stock = stock[stock["_code_norm"].notna() & (stock["_code_norm"] != "") & (stock["_code_norm"] != "nan")]

    # Detect and log duplicates BEFORE deduplication.
    dups = stock[stock.duplicated(subset="_code_norm", keep=False)]
    if not dups.empty:
        log.warning(
            "Duplicate codes found in stock report — using last occurrence as unit price.",
            extra={
                "duplicate_code_count": dups["_code_norm"].nunique(),
                "sample": dups["_code_norm"].unique()[:5].tolist(),
            },
        )

    # Keep last occurrence (most recent entry in the sheet).
    stock = stock.drop_duplicates(subset="_code_norm", keep="last")

    return dict(zip(stock["_code_norm"], stock[col_price]))


def _validate_join_columns(
    sales_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    col_sales_code: str,
    col_sales_qty: str,
    col_stock_code: str,
    col_stock_purchase_price: str,
) -> None:
    """Raise clear ValueError if any expected column is missing."""
    errors: list[str] = []

    for col in (col_sales_code, col_sales_qty):
        if col not in sales_df.columns:
            errors.append(f"Sales sheet is missing expected column '{col}'.")

    for col in (col_stock_code, col_stock_purchase_price):
        if col not in stock_df.columns:
            errors.append(f"Stock sheet is missing expected column '{col}'.")

    if errors:
        raise ValueError(
            "Purchase-price merge pre-check failed:\n"
            + "\n".join(f"  • {e}" for e in errors)
            + "\n\nCheck the COL_SALES_CODE / COL_SALES_QTY / COL_STOCK_CODE / "
              "COL_STOCK_PURCHASE_PRICE env vars match your actual sheet headers."
        )
