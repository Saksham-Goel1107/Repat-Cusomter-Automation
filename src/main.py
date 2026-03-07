"""
main.py — Entrypoint.

Execution flow:
  1. Load & validate configuration from environment.
  2. Read raw sale data from the input Google Sheet.
  3. Run the full analysis pipeline.
  4. Write all output DataFrames to the output Google Sheet.
  5. Append a run-log row (metadata + KPIs + status).
  6. Save charts to /app/logs/charts/.

Exit codes:
  0  — success
  1  — configuration / credentials error
  2  — data / runtime error
"""

import sys
import time
import traceback
from datetime import datetime, timezone
import os
from pathlib import Path

from src.config import load_config
from src.logger import setup_logger
from src.sheets_client import SheetsClient
from src.analysis import run_full_analysis, AnalysisResult
from src.data_prep import merge_purchase_prices


def main() -> int:
    # ── Bootstrap logger before config so errors are captured ───────────────
    log = setup_logger(level="INFO", fmt="json")

    # ── Development convenience: load .env file into environment if present
    # This allows running `python -m src.main` locally after copying .env.
    env_path = Path(".env")
    if env_path.exists():
        try:
            for raw in env_path.read_text(encoding="utf8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"')
                # Do not overwrite already-set environment variables.
                if k and os.environ.get(k) is None:
                    os.environ[k] = v
            log.debug("Loaded .env into environment", extra={"path": str(env_path)})
        except Exception as exc:
            log.warning("Failed to parse .env file; continuing without it", extra={"error": str(exc)})

    # ── Config ───────────────────────────────────────────────────────────────
    try:
        cfg = load_config()
    except (ValueError, FileNotFoundError) as exc:
        log.error("Configuration error", extra={"error": str(exc)})
        return 1

    # Re-configure logger with values from config.
    log = setup_logger(level=cfg.log_level, fmt=cfg.log_format)

    run_start = datetime.now(tz=timezone.utc)
    log.info("Job started", extra={"run_start": run_start.isoformat(), "dry_run": cfg.dry_run})

    # ── Sheets client ─────────────────────────────────────────────────────────
    try:
        client = SheetsClient(
            credentials_path=cfg.google_credentials_path,
            max_retries=cfg.sheets_max_retries,
            base_delay_s=cfg.sheets_retry_delay_s,
        )
    except Exception as exc:
        log.error("Failed to initialise Sheets client", extra={"error": str(exc)})
        return 1

    # ── Read sales sheet ───────────────────────────────────────────────────────────
    try:
        raw_df = client.read_sheet(cfg.input_spreadsheet_id, cfg.input_sheet_name)
    except Exception as exc:
        log.error("Failed to read sales sheet", extra={"error": str(exc)})
        return 2

    # ── Read stock sheet ───────────────────────────────────────────────────────────
    try:
        stock_df = client.read_sheet(cfg.stock_spreadsheet_id, cfg.stock_sheet_name)
    except Exception as exc:
        log.error("Failed to read stock sheet", extra={"error": str(exc)})
        return 2

    # ── Merge purchase prices: stock.purchasePrice × sales.quantity ────────────────
    try:
        raw_df = merge_purchase_prices(
            sales_df=raw_df,
            stock_df=stock_df,
            col_sales_code=cfg.col_sales_code,
            col_sales_qty=cfg.col_sales_qty,
            col_stock_code=cfg.col_stock_code,
            col_stock_purchase_price=cfg.col_stock_purchase_price,
        )
    except Exception as exc:
        log.error(
            "Purchase price merge failed",
            extra={"error": str(exc), "traceback": traceback.format_exc()},
        )
        return 2

    # ── Analyse ───────────────────────────────────────────────────────────────
    try:
        result: AnalysisResult = run_full_analysis(
            raw_df=raw_df,
            cashier_filter=cfg.cashier_filter,
            timezone=cfg.timezone,
        )
    except Exception as exc:
        log.error(
            "Analysis pipeline failed",
            extra={"error": str(exc), "traceback": traceback.format_exc()},
        )
        return 2

    # Chart generation disabled — no charts will be created in this deployment.

    # ── Write outputs ──────────────────────────────────────────────────────────
    if cfg.dry_run:
        log.info("DRY RUN — skipping all Sheets writes.")
    else:
        write_errors: list[str] = []

        outputs = [
            (cfg.out_tab_period_matrix,       result.customer_period_matrix),
            (cfg.out_tab_repeat_customers,     result.repeat_customers),
            (cfg.out_tab_unknown_customers,    result.unknown_customers_by_period),
            (cfg.out_tab_customer_summary,     result.customer_summary),
            (cfg.out_tab_segment_metrics,      result.segment_metrics),
            (cfg.out_tab_once_vs_repeat,       result.once_vs_repeat),
            (cfg.out_tab_repeat_bill_metrics,  result.repeat_bill_metrics),
            (cfg.out_tab_recovery_full,        result.recovery_full),
            (cfg.out_tab_recovery_jan1,        result.recovery_till_jan1),
            (cfg.out_tab_recovery_jan15,       result.recovery_till_jan15),
            (cfg.out_tab_recovery_feb1,        result.recovery_till_feb1),
        ]

        # Full sales export including computed `Purchase Price` (line-item level).
        # Write this after the summary tables so users see aggregated outputs first.
        try:
            if not (raw_df is None or (hasattr(raw_df, "empty") and raw_df.empty)):
                client.write_sheet(cfg.output_spreadsheet_id, cfg.out_tab_sales_with_purchase_price, raw_df)
                time.sleep(1.2)
        except Exception as exc:
            write_errors.append(f"{cfg.out_tab_sales_with_purchase_price}: {exc}")
            log.error("Failed to write sales-with-purchase-price sheet", extra={"error": str(exc)})

        for sheet_name, df in outputs:
            if df is None or (hasattr(df, "empty") and df.empty):
                log.warning("Skipping empty DataFrame", extra={"sheet": sheet_name})
                continue
            try:
                client.write_sheet(cfg.output_spreadsheet_id, sheet_name, df)
                # Polite pause to stay well below Sheets API rate limits.
                time.sleep(1.2)
            except Exception as exc:
                write_errors.append(f"{sheet_name}: {exc}")
                log.error(
                    "Failed to write sheet",
                    extra={"sheet": sheet_name, "error": str(exc)},
                )

        # ── Run-log row ────────────────────────────────────────────────────────
        run_end = datetime.now(tz=timezone.utc)
        elapsed = round((run_end - run_start).total_seconds(), 1)
        run_log_row = {
            "run_at_utc":            run_start.isoformat(),
            "elapsed_seconds":       elapsed,
            "status":                "PARTIAL_ERROR" if write_errors else "SUCCESS",
            "write_errors":          "; ".join(write_errors),
            "input_rows":            len(raw_df),
            "total_customers":       result.total_customers,
            "total_bills":           result.total_bills,
            "total_revenue":         result.total_revenue,
            "avg_bills_per_customer": result.avg_bills_per_customer,
            "avg_bill_size":         result.avg_bill_size,
            "avg_lifetime_profit":   result.avg_lifetime_profit,
        }
        try:
            client.append_row(
                cfg.output_spreadsheet_id,
                cfg.out_tab_run_log,
                run_log_row,
            )
        except Exception as exc:
            log.warning("Failed to write run log", extra={"error": str(exc)})

        if write_errors:
            log.error(
                "Job completed with write errors",
                extra={"errors": write_errors, "elapsed_s": elapsed},
            )
            return 2

    run_end = datetime.now(tz=timezone.utc)
    elapsed = round((run_end - run_start).total_seconds(), 1)
    log.info(
        "Job completed successfully",
        extra={
            "elapsed_s": elapsed,
            "total_customers": result.total_customers,
            "total_revenue":   result.total_revenue,
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
