"""
config.py — All configuration loaded from environment variables.
Fail fast at startup if required variables are missing.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Config:
    # ── Google credentials ─────────────────────────────────────────────────
    google_credentials_path: str

    # ── Input: Google Sheets source data ──────────────────────────────────
    input_spreadsheet_id: str
    input_sheet_name: str

    # ── Input: Stock / inventory sheet (purchase prices) ─────────────────
    stock_spreadsheet_id: str   # defaults to input_spreadsheet_id when not set
    stock_sheet_name: str

    # ── Column name mapping ───────────────────────────────────────────────
    # Sales sheet column that holds the item/product code.
    col_sales_code: str
    # Sales sheet column that holds the line-item quantity sold.
    col_sales_qty: str
    # Stock sheet column that holds the item/product code.
    col_stock_code: str
    # Stock sheet column that holds the unit purchase price.
    col_stock_purchase_price: str

    # ── Output: Google Sheets to write results ────────────────────────────
    output_spreadsheet_id: str

    # ── Business logic knobs ──────────────────────────────────────────────
    cashier_filter: str
    timezone: str

    # ── Operational config ────────────────────────────────────────────────
    log_level: str
    log_format: str       # "json" | "text"
    dry_run: bool         # When True, skip writing to Sheets (read + analyse only)

    # ── Sheets API back-off ────────────────────────────────────────────────
    sheets_max_retries: int
    sheets_retry_delay_s: float

    # ── Sheet tab names written to output spreadsheet ─────────────────────
    out_tab_period_matrix: str
    out_tab_repeat_customers: str
    out_tab_unknown_customers: str
    out_tab_customer_summary: str
    out_tab_segment_metrics: str
    out_tab_once_vs_repeat: str
    out_tab_repeat_bill_metrics: str
    out_tab_recovery_full: str
    out_tab_recovery_jan1: str
    out_tab_recovery_jan15: str
    out_tab_recovery_feb1: str
    out_tab_run_log: str
    out_tab_sales_with_purchase_price: str


def load_config() -> Config:
    """
    Build Config from environment variables.
    Raises ValueError immediately if a required variable is absent.
    """
    errors: list[str] = []

    def require(key: str) -> str:
        v = os.environ.get(key, "").strip()
        if not v:
            errors.append(f"Required env var '{key}' is not set.")
        return v

    def optional(key: str, default: str) -> str:
        return os.environ.get(key, default).strip() or default

    def optional_bool(key: str, default: bool) -> bool:
        raw = os.environ.get(key, "").strip().lower()
        if raw in ("1", "true", "yes"):
            return True
        if raw in ("0", "false", "no"):
            return False
        return default

    def optional_int(key: str, default: int) -> int:
        raw = os.environ.get(key, "").strip()
        try:
            return int(raw)
        except (ValueError, TypeError):
            return default

    def optional_float(key: str, default: float) -> float:
        raw = os.environ.get(key, "").strip()
        try:
            return float(raw)
        except (ValueError, TypeError):
            return default

    # Determine credentials path:
    # 1) If env var points to an existing file, use it.
    # 2) Else, search a set of common locations baked into the image or mounted.
    env_cred = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()
    candidate_paths = [
        env_cred,
        "/app/credentials/service.json",
        "/credentials/service.json",
        "/run/secrets/service.json",
        "/service.json",
    ]
    google_credentials_path = ""
    for p in candidate_paths:
        if not p:
            continue
        if os.path.isfile(p):
            google_credentials_path = p
            break

    # Fall back to the env value or the standard path for error messaging
    if not google_credentials_path:
        google_credentials_path = env_cred or "/app/credentials/service.json"

    cfg = Config(
        google_credentials_path=google_credentials_path,
        input_spreadsheet_id=require("INPUT_SPREADSHEET_ID"),
        input_sheet_name=optional("INPUT_SHEET_NAME", "Sheet1"),
        # Stock sheet — defaults to same spreadsheet as sales if not overridden.
        stock_spreadsheet_id=optional(
            "STOCK_SPREADSHEET_ID",
            os.environ.get("INPUT_SPREADSHEET_ID", "").strip(),
        ),
        stock_sheet_name=optional("STOCK_SHEET_NAME", "Stock"),
        # Column name knobs — override when your sheet headers differ.
        col_sales_code=optional("COL_SALES_CODE", "code"),
        col_sales_qty=optional("COL_SALES_QTY", "quantity"),
        col_stock_code=optional("COL_STOCK_CODE", "code"),
        col_stock_purchase_price=optional("COL_STOCK_PURCHASE_PRICE", "purchasePrice"),
        output_spreadsheet_id=require("OUTPUT_SPREADSHEET_ID"),
        cashier_filter=optional("CASHIER_FILTER", "sw-noida-cashier"),
        timezone=optional("TIMEZONE", "Asia/Kolkata"),
        log_level=optional("LOG_LEVEL", "INFO"),
        log_format=optional("LOG_FORMAT", "json"),
        dry_run=optional_bool("DRY_RUN", False),
        sheets_max_retries=optional_int("SHEETS_MAX_RETRIES", 5),
        sheets_retry_delay_s=optional_float("SHEETS_RETRY_DELAY_S", 2.0),
        out_tab_period_matrix=optional("OUT_TAB_PERIOD_MATRIX", "Customer_Period_Matrix"),
        out_tab_repeat_customers=optional("OUT_TAB_REPEAT_CUSTOMERS", "Repeat_Customers"),
        out_tab_unknown_customers=optional("OUT_TAB_UNKNOWN_CUSTOMERS", "Unknown_Customers"),
        out_tab_customer_summary=optional("OUT_TAB_CUSTOMER_SUMMARY", "Customer_Summary"),
        out_tab_segment_metrics=optional("OUT_TAB_SEGMENT_METRICS", "Segment_Metrics"),
        out_tab_once_vs_repeat=optional("OUT_TAB_ONCE_VS_REPEAT", "Once_vs_Repeat"),
        out_tab_repeat_bill_metrics=optional("OUT_TAB_REPEAT_BILL_METRICS", "Repeat_Bill_Metrics"),
        out_tab_recovery_full=optional("OUT_TAB_RECOVERY_FULL", "Recovery_Full_Range"),
        out_tab_recovery_jan1=optional("OUT_TAB_RECOVERY_JAN1", "Recovery_Till_Jan1"),
        out_tab_recovery_jan15=optional("OUT_TAB_RECOVERY_JAN15", "Recovery_Till_Jan15"),
        out_tab_recovery_feb1=optional("OUT_TAB_RECOVERY_FEB1", "Recovery_Till_Feb1"),
        out_tab_run_log=optional("OUT_TAB_RUN_LOG", "Run_Log"),
        out_tab_sales_with_purchase_price=optional("OUT_TAB_SALES_WITH_PURCHASE_PRICE", "Sales_With_PurchasePrice"),
    )

    if errors:
        raise ValueError("Configuration errors:\n" + "\n".join(f"  • {e}" for e in errors))

    # Validate credentials file exists (give helpful message if not)
    if not os.path.isfile(cfg.google_credentials_path):
        raise FileNotFoundError(
            "Google service account credentials not found at: "
            f"{cfg.google_credentials_path}\n"
            "Ensure the service.json is either baked into the image at one of the standard locations (e.g. /app/credentials/service.json) "
            "or mounted into the container at that path."
        )

    return cfg
