"""
analysis.py — Core business logic.

Refactored from the Colab notebook with the following improvements:
  • Bug fix: P3/P4 period assignment (original code assigned mask3 twice).
  • No plt.show() — charts are saved as PNG files, not displayed.
  • No hardcoded file paths — all I/O is DataFrame ↔ DataFrame.
  • Stronger customer key: mobile-prefixed vs name-prefixed to avoid collisions.
  • clear separation of concerns into small, testable functions.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pandas as pd

log = logging.getLogger("customer_analysis.analysis")


# ─────────────────────────────────────────────────────────────────────────────
# Return type
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AnalysisResult:
    """All output DataFrames in one container."""
    customer_period_matrix: pd.DataFrame
    repeat_customers: pd.DataFrame
    unknown_customers_by_period: pd.DataFrame
    customer_summary: pd.DataFrame
    segment_metrics: pd.DataFrame
    once_vs_repeat: pd.DataFrame
    repeat_bill_metrics: pd.DataFrame
    recovery_full: pd.DataFrame
    recovery_till_jan1: pd.DataFrame
    recovery_till_jan15: pd.DataFrame
    recovery_till_feb1: pd.DataFrame

    # Scalar KPIs (also appended to the run-log row)
    total_customers: int = 0
    total_bills: int = 0
    total_revenue: float = 0.0
    avg_bills_per_customer: float = 0.0
    avg_bill_size: float = 0.0
    avg_lifetime_profit: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Clean raw input DataFrame
# ─────────────────────────────────────────────────────────────────────────────

_REQUIRED_COLUMNS = {
    "date", "billed_by", "customerName", "customerMobile",
    "number", "totalAmount", "Purchase Price",
}

def validate_raw(df: pd.DataFrame) -> None:
    """Raise ValueError if expected columns are missing."""
    missing = _REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"Input sheet is missing required columns: {sorted(missing)}\n"
            f"Found columns: {sorted(df.columns)}"
        )


def clean_raw(df: pd.DataFrame, cashier_filter: str, timezone: str) -> pd.DataFrame:
    """
    Parse types, normalise timezone, filter by cashier.
    Returns a clean copy.
    """
    df = df.copy()

    # ── Dates ──────────────────────────────────────────────────────────────
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["date"] = df["date"].dt.tz_convert(timezone).dt.tz_localize(None)

    # ── Numeric columns ────────────────────────────────────────────────────
    for col in ("totalAmount", "Purchase Price"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # ── Cashier filter ─────────────────────────────────────────────────────
    before = len(df)
    df = df[df["billed_by"] == cashier_filter].copy()
    log.info(
        "Cashier filter applied",
        extra={
            "cashier": cashier_filter,
            "rows_before": before,
            "rows_after": len(df),
        },
    )

    if df.empty:
        raise ValueError(
            f"No rows match cashier filter '{cashier_filter}'. "
            "Check the CASHIER_FILTER env var and the billed_by column."
        )

    # ── Customer key ───────────────────────────────────────────────────────
    df["first_name"] = df["customerName"].apply(_clean_first_name)

    # Prefix-based keys prevent mobile "12345" colliding with a name "12345".
    df["customer_key"] = np.where(
        df["customerMobile"].notna() & (df["customerMobile"].astype(str).str.strip() != ""),
        "m_" + df["customerMobile"].astype(str).str.strip(),
        "n_" + df["first_name"].astype(str),
    )
    df.loc[
        df["customer_key"].isin(["n_nan", "n_None", "n_"]),
        "customer_key",
    ] = "unknown_customer"

    return df


def _clean_first_name(x) -> str:
    """Lower-case alphabetic first token; returns NaN when unresolvable."""
    if pd.isna(x):
        return np.nan
    cleaned = re.sub(r"[^A-Za-z\s]", "", str(x)).strip()
    if not cleaned:
        return np.nan
    return cleaned.split()[0].lower()


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Collapse line items → bill level
# ─────────────────────────────────────────────────────────────────────────────

def build_bill_level(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate line-item rows into one row per bill number.
    Adds: bill_date, customer_key, order_value, total_purchase_price, profit.
    """
    bill_level = (
        df.groupby("number", as_index=False)
        .agg(
            bill_date=("date", "min"),
            customer_key=("customer_key", "first"),
            first_name=("first_name", "first"),
            customerMobile=("customerMobile", "first"),
            order_value=("totalAmount", "sum"),
            total_purchase_price=("Purchase Price", "sum"),
        )
    )
    bill_level["profit"] = bill_level["order_value"] - bill_level["total_purchase_price"]
    return bill_level


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Period matrix (P1–P4)
# ─────────────────────────────────────────────────────────────────────────────

def _derive_period_boundaries(bill_level: pd.DataFrame) -> dict:
    """
    Auto-derive date window boundaries from the data rather than hard-coding
    the year.  Mirrors the logic at the bottom of the original notebook.
    """
    data_min = bill_level["bill_date"].min().normalize()
    year_start = data_min.year
    y1 = year_start + 1   # next calendar year

    return {
        # --- period boundaries (closed on left, open on right) ---
        "p1_start": pd.Timestamp(y1,  1,  1),
        "p2_start": pd.Timestamp(y1,  1, 16),
        "p3_start": pd.Timestamp(y1,  2, 16),
        "p4_start": pd.Timestamp(y1,  2, 28),
        "p4_end":   pd.Timestamp(y1,  3, 15),
        # --- rolling-window start ---
        "data_start": pd.Timestamp(year_start, 11, 18),
        # --- rolling-window sub-windows ---
        "jan_1":  pd.Timestamp(y1, 1,  1),
        "jan_15": pd.Timestamp(y1, 1, 15),
        "feb_1":  pd.Timestamp(y1, 2,  1),
        "end_full": pd.Timestamp(y1, 2, 15),
    }


def build_period_matrix(bill_level: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
        customer_period_matrix — wide pivot (all customers, all periods).
        repeat_customers       — subset of customers active in >1 period.
        unknown_period_matrix  — period breakdown for unknown_customer.
    """
    b = _derive_period_boundaries(bill_level)
    bl = bill_level.copy()

    # ── Assign periods ──────────────────────────────────────────────────────
    bl["period"] = pd.NA

    # BUG FIX vs original: original assigned mask3 twice (P3 & P4 both used mask3).
    mask1 = (bl["bill_date"] >= b["p1_start"]) & (bl["bill_date"] < b["p2_start"])
    mask2 = (bl["bill_date"] >= b["p2_start"]) & (bl["bill_date"] < b["p3_start"])
    mask3 = (bl["bill_date"] >= b["p3_start"]) & (bl["bill_date"] < b["p4_start"])
    mask4 = (bl["bill_date"] >= b["p4_start"]) & (bl["bill_date"] < b["p4_end"])

    bl.loc[mask1, "period"] = "P1_Jan01_Jan15"
    bl.loc[mask2, "period"] = "P2_Jan16_Feb15"
    bl.loc[mask3, "period"] = "P3_Feb16_Feb27"
    bl.loc[mask4, "period"] = "P4_Feb28_Mar14"

    bl = bl.dropna(subset=["period"])

    if bl.empty:
        log.warning("No bills fall within any defined period — period matrix will be empty.")
        empty = pd.DataFrame()
        return empty, empty, empty

    # ── Pivot ───────────────────────────────────────────────────────────────
    agg = (
        bl.groupby(["customer_key", "period"])
        .agg(
            bills=("number", "nunique"),
            total_sales=("order_value", "sum"),
            total_purchase_price=("total_purchase_price", "sum"),
            total_profit=("profit", "sum"),
        )
        .reset_index()
    )

    matrix = agg.pivot(
        index="customer_key",
        columns="period",
        values=["bills", "total_sales", "total_purchase_price", "total_profit"],
    ).fillna(0)

    matrix.columns = [f"{metric}_{period}" for metric, period in matrix.columns]

    matrix["lifetime_sales"] = matrix.filter(like="total_sales_").sum(axis=1)
    matrix["lifetime_cost"] = matrix.filter(like="total_purchase_price_").sum(axis=1)
    matrix["lifetime_profit"] = matrix.filter(like="total_profit_").sum(axis=1)

    # ── Repeat customers — active in ≥2 periods ─────────────────────────────
    bill_cols = [c for c in matrix.columns if c.startswith("bills_")]
    active_periods = (matrix[bill_cols] > 0).astype(int).sum(axis=1)
    repeat_customers = matrix[active_periods > 1].copy()

    # ── Unknown customers aggregated by period ───────────────────────────────
    unknown_period = (
        bl[bl["customer_key"] == "unknown_customer"]
        .groupby("period")
        .agg(
            bills=("number", "nunique"),
            total_sales=("order_value", "sum"),
            total_purchase_price=("total_purchase_price", "sum"),
            total_profit=("profit", "sum"),
        )
    )

    return matrix, repeat_customers, unknown_period


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Customer summary + frequency segments
# ─────────────────────────────────────────────────────────────────────────────

def build_customer_summary(bill_level: pd.DataFrame) -> pd.DataFrame:
    """Per-customer: total bills, spend, AOV, repeat segment, repeat flag."""
    summary = (
        bill_level.groupby("customer_key")
        .agg(
            total_bills=("number", "nunique"),
            total_spend=("order_value", "sum"),
            total_cost=("total_purchase_price", "sum"),
            avg_order_value=("order_value", "mean"),
        )
    )
    # Lifetime profit per customer
    summary["lifetime_profit"] = summary["total_spend"] - summary["total_cost"]
    summary["avg_profit_per_bill"] = summary.apply(
        lambda r: (r["lifetime_profit"] / r["total_bills"]) if r["total_bills"] else 0.0,
        axis=1,
    )

    summary["repeat_segment"] = summary["total_bills"].apply(_segment_customer)
    summary["repeat_flag"] = np.where(summary["total_bills"] > 1, "repeat", "once")
    return summary


def _segment_customer(n: int) -> str:
    if n == 1:
        return "once"
    if n == 2:
        return "twice"
    if 3 <= n <= 4:
        return "3-4"
    return "5+"


def build_segment_metrics(customer_summary: pd.DataFrame) -> pd.DataFrame:
    return (
        customer_summary.groupby("repeat_segment")
        .agg(
            customers=("total_bills", "count"),
            avg_bills_per_customer=("total_bills", "mean"),
            avg_order_value=("avg_order_value", "mean"),
            total_revenue=("total_spend", "sum"),
            total_cost=("total_cost", "sum"),
            total_profit=("lifetime_profit", "sum"),
            avg_profit_per_customer=("lifetime_profit", "mean"),
        )
        .sort_index()
    )


def build_once_vs_repeat(customer_summary: pd.DataFrame) -> pd.DataFrame:
    return (
        customer_summary.groupby("repeat_flag")
        .agg(
            customers=("total_bills", "count"),
            avg_bills=("total_bills", "mean"),
            avg_order_value=("avg_order_value", "mean"),
            total_revenue=("total_spend", "sum"),
            total_cost=("total_cost", "sum"),
            total_profit=("lifetime_profit", "sum"),
            avg_profit_per_customer=("lifetime_profit", "mean"),
        )
    )


def build_repeat_bill_metrics(
    bill_level: pd.DataFrame,
    customer_summary: pd.DataFrame,
) -> pd.DataFrame:
    bl = bill_level.merge(
        customer_summary[["repeat_flag"]],
        left_on="customer_key",
        right_index=True,
        how="left",
    )
    metrics = (
        bl.groupby("repeat_flag")
        .agg(
            total_customers=("customer_key", "nunique"),
            total_bills=("number", "nunique"),
            avg_bill_size=("order_value", "mean"),
            total_profit=("profit", "sum"),
            avg_profit_per_bill=("profit", "mean"),
        )
    )
    metrics["avg_bills_per_customer"] = (
        metrics["total_bills"] / metrics["total_customers"]
    )
    # per-customer profit
    metrics["avg_profit_per_customer"] = (
        metrics["total_profit"] / metrics["total_customers"]
    )
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — Exact-visit recovery reports (four rolling windows)
# ─────────────────────────────────────────────────────────────────────────────

def _calc_metrics(df_subset: pd.DataFrame, label: str) -> dict:
    total_sales = df_subset["order_value"].sum()
    total_cost = df_subset.get("total_purchase_price", pd.Series(dtype=float)).sum()
    total_bills = df_subset["number"].nunique()
    total_customers = df_subset["customer_key"].nunique()
    total_profit = total_sales - total_cost
    a = total_sales / total_bills if total_bills else 0.0
    b = total_bills / total_customers if total_customers else 0.0
    return {
        "segment": label,
        "customers": total_customers,
        "total_bills": total_bills,
        "total_sales": round(total_sales, 2),
        "total_cost": round(float(total_cost), 2),
        "total_profit": round(float(total_profit), 2),
        "avg_order_value_a": round(a, 2),
        "avg_bills_per_customer_b": round(b, 3),
        "avg_recovery_c": round(a * b, 2),
        "avg_profit_per_customer": round((total_profit / total_customers) if total_customers else 0.0, 2),
    }


def build_recovery_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build exact-visit cohort recovery report for a given df slice.
    df must already be filtered to the date window you want.
    """
    if df.empty:
        log.warning("build_recovery_report: empty input — returning empty frame.")
        return pd.DataFrame()

    bl = build_bill_level(df)

    customer_visit_counts = (
        bl.groupby("customer_key")
        .agg(total_bills=("number", "nunique"))
    )
    bl = bl.merge(customer_visit_counts, left_on="customer_key", right_index=True, how="left")

    valid = customer_visit_counts.loc[customer_visit_counts.index != "unknown_customer"]
    visit_values = sorted(valid["total_bills"].unique())

    rows = []

    # All valid customers (≥1 visit)
    rows.append(_calc_metrics(
        bl[bl["customer_key"] != "unknown_customer"],
        ">=1_visits",
    ))

    # Exact cohorts
    for v in visit_values:
        subset = bl[
            (bl["total_bills"] == v) &
            (bl["customer_key"] != "unknown_customer")
        ]
        if subset.empty:
            continue
        rows.append(_calc_metrics(subset, f"{v}_visits"))

    # Unknown bucket
    rows.append(_calc_metrics(
        bl[bl["customer_key"] == "unknown_customer"],
        "unknown_customer",
    ))

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Step 6 — Charts (saved to disk, not shown interactively)
# ─────────────────────────────────────────────────────────────────────────────

def save_charts(result: "AnalysisResult", output_dir: str = "/app/logs") -> None:
    """
    Render and save all charts as PNG files to output_dir.
    Called after the main analysis; safe to skip if charts are not needed.
    """
    # Chart generation removed — this function is intentionally a no-op.
    log.info("Chart generation disabled; no charts created")


# ─────────────────────────────────────────────────────────────────────────────
# Master orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def run_full_analysis(
    raw_df: pd.DataFrame,
    cashier_filter: str = "sw-noida-cashier",
    timezone: str = "Asia/Kolkata",
) -> AnalysisResult:
    """
    Run the complete analysis pipeline on a raw input DataFrame.
    Returns an AnalysisResult with all output DataFrames populated.
    """
    log.info("Analysis started", extra={"raw_rows": len(raw_df)})

    # 1. Validate & clean
    validate_raw(raw_df)
    df = clean_raw(raw_df, cashier_filter=cashier_filter, timezone=timezone)

    # 2. Bill-level aggregation
    bill_level = build_bill_level(df)
    log.info("Bill-level built", extra={"bills": len(bill_level)})

    # 3. Period matrix
    cpm, repeat_cust, unknown_period = build_period_matrix(bill_level)

    # 4. Customer summary & segment tables
    customer_summary = build_customer_summary(bill_level)
    segment_metrics = build_segment_metrics(customer_summary)
    once_vs_repeat = build_once_vs_repeat(customer_summary)
    repeat_bill_metrics = build_repeat_bill_metrics(bill_level, customer_summary)

    # 5. Rolling-window recovery reports
    b = _derive_period_boundaries(bill_level)
    ds = b["data_start"]

    def _slice(end_ts):
        return df[(df["date"] >= ds) & (df["date"] <= end_ts)]

    recovery_full  = build_recovery_report(_slice(b["end_full"]))
    recovery_jan1  = build_recovery_report(_slice(b["jan_1"]))
    recovery_jan15 = build_recovery_report(_slice(b["jan_15"]))
    recovery_feb1  = build_recovery_report(_slice(b["feb_1"]))

    # 6. Scalar KPIs
    total_customers = customer_summary.shape[0]
    total_bills_n = bill_level["number"].nunique()
    total_revenue = bill_level["order_value"].sum()

    result = AnalysisResult(
        customer_period_matrix=cpm,
        repeat_customers=repeat_cust,
        unknown_customers_by_period=unknown_period,
        customer_summary=customer_summary,
        segment_metrics=segment_metrics,
        once_vs_repeat=once_vs_repeat,
        repeat_bill_metrics=repeat_bill_metrics,
        recovery_full=recovery_full,
        recovery_till_jan1=recovery_jan1,
        recovery_till_jan15=recovery_jan15,
        recovery_till_feb1=recovery_feb1,
        total_customers=total_customers,
        total_bills=total_bills_n,
        total_revenue=round(float(total_revenue), 2),
        avg_bills_per_customer=round(total_bills_n / total_customers, 3) if total_customers else 0.0,
        avg_bill_size=round(float(total_revenue) / total_bills_n, 2) if total_bills_n else 0.0,
        avg_lifetime_profit=round(
            float(cpm["lifetime_profit"].mean()) if not cpm.empty else 0.0, 2
        ),
    )

    log.info(
        "Analysis complete",
        extra={
            "total_customers": result.total_customers,
            "total_bills": result.total_bills,
            "total_revenue": result.total_revenue,
            "avg_bill_size": result.avg_bill_size,
            "avg_bills_per_customer": result.avg_bills_per_customer,
        },
    )
    return result
