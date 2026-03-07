"""
sheets_client.py — Google Sheets I/O with exponential back-off and rate-limit handling.

All Sheets API calls go through _retry() — every transient error
(429, 5xx, transport timeout) is retried with jittered back-off.
"""

import logging
import math
import random
import time
from typing import Optional

import gspread
import pandas as pd
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

# Read-write scopes required for Sheets + Drive metadata.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]

log = logging.getLogger("customer_analysis.sheets")


class SheetsClient:
    """
    Thin wrapper around gspread that adds:
      • Exponential back-off with full jitter for all API calls.
      • Clear-before-write to ensure idempotent output updates.
      • Auto-creation of missing worksheet tabs.
      • Type-safe DataFrame serialisation (NaN → "", numpy types → Python native).
    """

    def __init__(
        self,
        credentials_path: str,
        max_retries: int = 5,
        base_delay_s: float = 2.0,
    ) -> None:
        self._max_retries = max_retries
        self._base_delay_s = base_delay_s
        creds = Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )
        self._gc = gspread.authorize(creds)
        log.debug("Google Sheets client initialised", extra={"credentials_path": credentials_path})

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def read_sheet(
        self,
        spreadsheet_id: str,
        sheet_name: str,
    ) -> pd.DataFrame:
        """
        Read an entire worksheet and return a pandas DataFrame.
        The first row is used as column headers.
        """
        log.info(
            "Reading sheet",
            extra={"spreadsheet_id": spreadsheet_id, "sheet_name": sheet_name},
        )
        worksheet = self._retry(
            lambda: self._open_worksheet(spreadsheet_id, sheet_name, create=False)
        )
        df: pd.DataFrame = self._retry(
            lambda: get_as_dataframe(worksheet, evaluate_formulas=True, dtype=str)
        )
        # Drop fully-empty rows that gspread sometimes appends.
        df = df.dropna(how="all").reset_index(drop=True)
        log.info(
            "Sheet read complete",
            extra={
                "spreadsheet_id": spreadsheet_id,
                "sheet_name": sheet_name,
                "rows": len(df),
                "cols": len(df.columns),
            },
        )
        return df

    def write_sheet(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        df: pd.DataFrame,
        include_index: bool = False,
    ) -> None:
        """
        Clear the worksheet and write the DataFrame from row 1, column A.
        Creates the sheet tab if it doesn't exist.
        """
        log.info(
            "Writing sheet",
            extra={
                "spreadsheet_id": spreadsheet_id,
                "sheet_name": sheet_name,
                "rows": len(df),
                "cols": len(df.columns),
            },
        )
        worksheet = self._retry(
            lambda: self._open_worksheet(spreadsheet_id, sheet_name, create=True)
        )
        # Clear existing data first — idempotent behaviour on re-runs.
        self._retry(lambda: worksheet.clear())

        clean_df = _prepare_df_for_sheets(df, include_index=include_index)

        # Convert cleaned DataFrame into a list-of-lists (header + rows)
        headers = list(clean_df.columns)
        values = [headers]
        # Ensure every row is a plain Python scalar (strings/nums/empty strings)
        for _, row in clean_df.iterrows():
            values.append([row[c] if row[c] is not None else "" for c in headers])

        # Resize the sheet to fit the data and perform an atomic update.
        def _upd():
            try:
                worksheet.resize(rows=len(values), cols=len(headers))
            except Exception:
                # Some worksheets may not allow resize; ignore and proceed to update values.
                pass
            return worksheet.update("A1", values)

        self._retry(lambda: _upd())
        log.info(
            "Sheet write complete",
            extra={"spreadsheet_id": spreadsheet_id, "sheet_name": sheet_name},
        )

    def append_row(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        row: dict,
    ) -> None:
        """Append a single dict as a new row (used for the run log)."""
        worksheet = self._retry(
            lambda: self._open_worksheet(spreadsheet_id, sheet_name, create=True)
        )
        # If sheet is empty, write header first.
        existing = self._retry(lambda: worksheet.get_all_values())
        if not existing:
            self._retry(lambda: worksheet.append_row(list(row.keys())))
        self._retry(lambda: worksheet.append_row(
            [str(v) if v is not None else "" for v in row.values()]
        ))

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _open_worksheet(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        create: bool = False,
    ) -> gspread.Worksheet:
        try:
            spreadsheet = self._gc.open_by_key(spreadsheet_id)
        except SpreadsheetNotFound:
            raise SpreadsheetNotFound(
                f"Spreadsheet '{spreadsheet_id}' not found. "
                "Check the ID and that the service account has access."
            )
        try:
            return spreadsheet.worksheet(sheet_name)
        except WorksheetNotFound:
            if create:
                log.info(
                    "Worksheet not found — creating",
                    extra={"sheet_name": sheet_name},
                )
                return spreadsheet.add_worksheet(
                    title=sheet_name, rows=5000, cols=50
                )
            raise

    def _retry(self, fn, *args, **kwargs):
        """
        Call fn() with exponential back-off + full jitter.
        Retries on:
          • gspread.APIError with status 429 (rate limit) or 5xx.
          • ConnectionError / TimeoutError (transient network issues).
        """
        delay = self._base_delay_s
        last_exc: Optional[Exception] = None

        for attempt in range(1, self._max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except APIError as exc:
                status = exc.response.status_code if exc.response else 0
                if status == 429 or status >= 500:
                    last_exc = exc
                    log.warning(
                        "Sheets API transient error — retrying",
                        extra={
                            "attempt": attempt,
                            "status": status,
                            "delay_s": round(delay, 2),
                        },
                    )
                else:
                    # Client error (4xx except 429) — do not retry.
                    raise
            except (ConnectionError, TimeoutError, OSError) as exc:
                last_exc = exc
                log.warning(
                    "Network error — retrying",
                    extra={"attempt": attempt, "error": str(exc), "delay_s": round(delay, 2)},
                )

            if attempt < self._max_retries:
                # Full jitter: sleep between 0 and delay seconds.
                jitter = random.uniform(0, delay)
                time.sleep(jitter)
                delay = min(delay * 2, 64.0)   # cap at 64 s

        raise RuntimeError(
            f"Sheets API call failed after {self._max_retries} attempts."
        ) from last_exc


# ─────────────────────────────────────────────────────────────────────────────
# Serialisation utility
# ─────────────────────────────────────────────────────────────────────────────

def _prepare_df_for_sheets(df: pd.DataFrame, include_index: bool) -> pd.DataFrame:
    """
    Flatten MultiIndex, reset index, convert all values to JSON-safe Python
    types (Google Sheets API rejects numpy scalars and NaN/Inf).
    """
    df = df.copy()

    # Flatten MultiIndex columns (produced by pivot)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join(str(c) for c in col).strip("_")
            for col in df.columns.values
        ]

    if include_index:
        df = df.reset_index()
    else:
        # The index is often the customer_key — reset so it becomes a column.
        if df.index.name or not isinstance(df.index, pd.RangeIndex):
            df = df.reset_index()

    # Convert every cell to a JSON-safe scalar.
    df = df.where(pd.notnull(df), other=None)
    # Use per-column Series.apply instead of DataFrame.applymap to avoid
    # environments where applymap may not be available or behaves oddly.
    for col in df.columns:
        df[col] = df[col].apply(_to_python_scalar)

    return df


def _to_python_scalar(val):
    """Convert numpy / pandas scalars → Python native; None/NaN → empty string."""
    import numpy as np

    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return ""
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return round(float(val), 6)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    return val
