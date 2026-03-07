# Customer Repeat Analysis — Production Automation

Dockerised, cron-scheduled service that reads raw sale data from Google Sheets,
runs the full repeat-customer analysis pipeline, and writes all output tables
back to Google Sheets automatically.

---

## Architecture

```
Google Sheets (Input)
       │  gspread read
       ▼
  clean_raw()
       │
  build_bill_level()
       ├──► build_period_matrix()     → Customer_Period_Matrix, Repeat_Customers, Unknown_Customers
       ├──► build_customer_summary()  → Customer_Summary
       ├──► build_segment_metrics()   → Segment_Metrics
       ├──► build_once_vs_repeat()    → Once_vs_Repeat
       ├──► build_repeat_bill_metrics()→ Repeat_Bill_Metrics
       └──► build_recovery_report()  → Recovery_Full_Range, Recovery_Till_Jan1/Jan15/Feb1
                                       (4 rolling-window cohort reports)
       │  gspread write (with exponential back-off)
       ▼
Google Sheets (Output) + Run_Log tab
```

---

## Quick Start

### 1. Google Cloud setup

1. Create a **Service Account** in Google Cloud Console.
2. Give it the **Editor** role on both your input and output spreadsheets
   (Share the sheets with the service account email address).
3. Create a JSON key for the service account and save it as
   `credentials/service.json`.

### 2. Configure

```bash
cp env.example .env
# Edit .env — set INPUT_SPREADSHEET_ID, OUTPUT_SPREADSHEET_ID at minimum.
```

### 3. Input sheet format

**Sales report** tab must have these columns (exact names, or configure via env vars):

| Column           | Type          | Notes                                              |
|------------------|---------------|----------------------------------------------------|
| `date`           | datetime/text | ISO or Excel timestamp                             |
| `billed_by`      | text          | Filtered by `CASHIER_FILTER`                       |
| `customerName`   | text          | May be blank                                       |
| `customerMobile` | text/number   | May be blank                                       |
| `number`         | text/number   | Bill / invoice number                              |
| `totalAmount`    | number        | Line-item sale amount                              |
| `code`           | text/number   | Item/product code — joined to stock report         |
| `quantity`       | number        | Quantity sold per line item                        |

> `Purchase Price` is now **computed automatically** as `purchasePrice × quantity`
> using the stock report — do not include it manually in the sales sheet.

**Stock report** tab must have at minimum:

| Column          | Type   | Notes                                              |
|-----------------|--------|----------------------------------------------------|
| `code`          | text   | Item/product code — must match sales `code` column |
| `purchasePrice` | number | Unit cost/purchase price for the item              |

Column names are configurable — see `COL_SALES_CODE`, `COL_SALES_QTY`,
`COL_STOCK_CODE`, `COL_STOCK_PURCHASE_PRICE` in the env vars table below.

### 4. Build and run

```bash
# Build the image
docker compose build

# Run analysis once right now (smoke test)
docker compose run --rm customer-analysis-oneshot

# Start the cron scheduler (runs daily at 01:00 UTC by default)
docker compose up -d customer-analysis

# View live logs
docker compose logs -f customer-analysis
```

### 5. Change the cron schedule

Edit `CRON_SCHEDULE` in `.env` (standard 5-field UTC cron syntax), then
rebuild:

```bash
# Example: run at 20:30 UTC (02:00 IST)
CRON_SCHEDULE=30 20 * * *
docker compose build && docker compose up -d
```

---

## Output sheets written

| Tab name                | Contents                                      |
|-------------------------|-----------------------------------------------|
| `Customer_Period_Matrix`| Wide pivot: bills/sales/profit per period     |
| `Repeat_Customers`      | Subset active in ≥2 periods                   |
| `Unknown_Customers`     | Unknown-key bills aggregated by period        |
| `Customer_Summary`      | Per-customer bills, spend, AOV, segment       |
| `Segment_Metrics`       | Cohort metrics: once / twice / 3-4 / 5+       |
| `Once_vs_Repeat`        | Side-by-side once vs repeat comparison        |
| `Repeat_Bill_Metrics`   | Bill-level metrics split by repeat flag       |
| `Recovery_Full_Range`   | Exact-visit recovery report (full window)     |
| `Recovery_Till_Jan1`    | … up to Jan 1                                 |
| `Recovery_Till_Jan15`   | … up to Jan 15                                |
| `Recovery_Till_Feb1`    | … up to Feb 1                                 |
| `Run_Log`               | One row per execution with KPIs + status      |

---

## Local development (without Docker)

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
cp env.example .env             # fill in values

# Set env vars (Windows PowerShell)
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#=]+?)\s*=\s*(.*)') {
        [System.Environment]::SetEnvironmentVariable($Matches[1], $Matches[2])
    }
}

python -m src.main
```

---

## Environment variables reference

| Variable                      | Required | Default                              | Description                              |
|-------------------------------|----------|--------------------------------------|------------------------------------------|
| `INPUT_SPREADSHEET_ID`        | ✅       | —                                    | Sales spreadsheet ID                     |
| `OUTPUT_SPREADSHEET_ID`       | ✅       | —                                    | Destination spreadsheet ID               |
| `INPUT_SHEET_NAME`            |          | `Sheet1`                             | Tab name in sales spreadsheet            |
| `STOCK_SPREADSHEET_ID`        |          | same as `INPUT_SPREADSHEET_ID`       | Stock spreadsheet ID (if different)      |
| `STOCK_SHEET_NAME`            |          | `Stock`                              | Tab name for stock/inventory data        |
| `COL_SALES_CODE`              |          | `code`                               | Column in sales sheet for item code      |
| `COL_SALES_QTY`               |          | `quantity`                           | Column in sales sheet for quantity       |
| `COL_STOCK_CODE`              |          | `code`                               | Column in stock sheet for item code      |
| `COL_STOCK_PURCHASE_PRICE`    |          | `purchasePrice`                      | Column in stock sheet for unit cost      |
| `GOOGLE_CREDENTIALS_PATH`     |          | `/app/credentials/service.json`      | Path to service account JSON key         |
| `CASHIER_FILTER`              |          | `sw-noida-cashier`                   | `billed_by` value to filter rows         |
| `TIMEZONE`                    |          | `Asia/Kolkata`                       | Timezone for date normalisation          |
| `CRON_SCHEDULE`               |          | `0 1 * * *`                          | Cron schedule (UTC)                      |
| `DRY_RUN`                     |          | `false`                              | Read + analyse only, no Sheets writes    |
| `LOG_LEVEL`                   |          | `INFO`                               | `DEBUG` / `INFO` / `WARNING` / `ERROR`   |
| `LOG_FORMAT`                  |          | `json`                               | `json` (structured) or `text`            |
| `SHEETS_MAX_RETRIES`          |          | `5`                                  | Max retry attempts on transient errors   |
| `SHEETS_RETRY_DELAY_S`        |          | `2.0`                                | Base back-off delay in seconds           |

---

## Bug fixes vs original notebook

| Original                                  | Fixed                                     |
|-------------------------------------------|-------------------------------------------|
| `mask3` was assigned to both P3 and P4    | Separate `mask3` / `mask4` used correctly |
| Duplicate imports scattered across cells  | Single clean import block                 |
| Notebook used interactive plotting (`plt.show()`) | Visual generation removed; pipeline is data-only |
| No error handling or retry logic          | Full exponential back-off on Sheets API   |
| Hardcoded file paths                      | All I/O via Google Sheets + env vars      |
