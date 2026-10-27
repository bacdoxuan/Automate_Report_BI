# Automate Report BI (Vietnamobile)

## Project Overview

This project is an automated Business Intelligence (BI) ETL pipeline for Vietnamobile. It automates the collection, transformation, and aggregation of 3G and 4G telecommunications network KPI data across Ericsson and ZTE equipment vendors, consolidating daily performance statistics into a rolling master report (`Aggregate.xlsx`) for PowerBI dashboards.

The system includes:
- **Exchange Web Services (EWS) Integration:** Automatically downloads email attachments from Microsoft Exchange on-premises servers using NTLM authentication across personal and shared mailboxes.
- **Automated Archive Extraction:** Unpacks raw ZIP archives to extract hourly and daily network performance files.
- **Modular Data Processing:** Specialized ETL processors for Ericsson (3G/4G) and ZTE (3G/4G), calculating busy hour metrics, throughput, users, and voice/data traffic.
- **Master Excel Aggregation:** Consolidates all metrics at the site level, maintains a rolling 30-day historical window, and uses chunked writing to prevent memory exhaustion.
- **Web Dashboard & Persistent Scheduler:** A Gradio-based web interface backed by `APScheduler` and SQLite (`schedules.db`) for running manual tasks, managing recurring cron schedules, and inspecting run histories.
- **Automated Notification & Logging:** Detailed run logging (`Log/log_YYYY-MM-DD.txt`) and email alerts (with attached logs) for both successful runs and failures.

---

## Architecture & System Workflow

```
app_gradio.py (Gradio Web UI + APScheduler)
   │
   └── Subprocess Execution ──> script.py (Main Orchestrator)
                                  ├─ exchange_lib.py     (NTLM EWS connection & 2-pass email download)
                                  ├─ extract_zippy.py    (Extracts ZIP files in downloads/)
                                  ├─ processing_*.py     (4 Vendor/Tech processors: 3G/4G × Ericsson/ZTE)
                                  ├─ notification.py     (Email status alerts with attached log file)
                                  └─ Aggregate.xlsx      (Rolling 30-day master report; chunked write)
```

### End-to-End Execution Flow (`script.py`)

1. **Logging Setup:** Creates or appends to `Log/log_YYYY-MM-DD.txt` (named after `process_date`), redirecting both `stdout` and `stderr` to terminal and log file simultaneously.
2. **Directory Cleanup:** Purges and recreates the `downloads/` directory before each run to avoid processing stale data.
3. **Two-Pass Email Retrieval (`exchange_lib.py`):**
   - **Pass 1 (Personal Mailbox):** Searches `inbox` for emails from `bac.dx@vietnamobile.com.vn` matching 4 Ericsson subjects:
     - `Automate_3G_Throughput` (XLSX)
     - `Automate_3G_Traffic_User` (XLSX)
     - `Automate_VoLTE_Traffic_Ericsson` (XLSX)
     - `Automate_North_LTE_Traffic_Data` (XLSX)
   - **Pass 2 (Shared Mailbox):** Searches `inbox` for emails from `vnm.performance.reporting@vietnamobile.com.vn` matching 8 ZTE subjects:
     - `[EXTERNAL]Task name:Automate_3G_ZTE_Traffic_EMS1_WD`
     - `[EXTERNAL]Task name:Automate_3G_ZTE_User_TP_EMS1_BH`
     - `[EXTERNAL]Task name:Automate_4G_ZTE_Traffic_EMS1_WD`
     - `[EXTERNAL]Task name:Automate_4G_ZTE_User_TP_EMS1_BH`
     - `[EXTERNAL]Task name:Automate_3G_ZTE_Traffic_EMS2_WD`
     - `[EXTERNAL]Task name:Automate_3G_ZTE_User_TP_EMS2_BH`
     - `[EXTERNAL]Task name:Automate_4G_ZTE_Traffic_EMS2_WD`
     - `[EXTERNAL]Task name:Automate_4G_ZTE_User_TP_EMS2_BH`
4. **Archive Extraction (`extract_zippy.py`):** Extracts all ZIP files in `downloads/` into their respective CSV files.
5. **Data Processing (4 Modules):** Each module loads, transforms, standardizes, cleans, and aggregates to site-level:
   - `processing_3G_Ericsson.py` (`Ericsson3GProcessor`)
   - `processing_3G_ZTE.py` (`ZTE3GProcessor`)
   - `processing_4G_Ericsson.py` (`Ericsson4GProcessor`)
   - `processing_4G_ZTE.py` (`ZTE4GProcessor`)
6. **Data Merging:**
   - Concatenates Ericsson + ZTE for 3G (`3G_Site_Data`) and 4G (`4G_Site_Data`).
   - Performs outer merge on `SiteID` (missing values filled with 0).
   - Adds the `Date` column (formatted as `process_date`).
7. **Consolidated Excel Export (`Aggregate.xlsx`):**
   - Appends today's data to existing records in `../Aggregate.xlsx`.
   - Filters out records older than 30 days.
   - Writes the dataset using `openpyxl` with `write_only=True` in 10,000-row chunks to a temporary `.tmp` file, then atomically replaces `Aggregate.xlsx`.
8. **Backup Replication:** Copies `Aggregate.xlsx` to `PATH_TO_COPY` (e.g. `D:/Project/Automate PowerBI - Display Site Information/Backup_Aggregate/`).
9. **Status Notification (`notification.py`):** Sends a success/failure notification email with execution details and attached log file to `RESULT_RECEIVER_LIST`.

### Date Logic & Timezones

- **Shift Rule:** Performance data for day **$T$** (`process_date`) is generated and emailed on day **$T+1$** (`email_search_date`).
- Default: `process_date` defaults to yesterday; `email_search_date = process_date + 1 day`.
- Timezone: Microsoft Exchange searches use the `Asia/Ho_Chi_Minh` timezone boundary (`00:00:00` to `23:59:59`).

---

## File Structure & Component Roles

| File / Directory | Purpose |
|---|---|
| `app_gradio.py` | Primary Web UI (Gradio) for triggering runs, configuring schedules (APScheduler), and viewing logs/history. |
| `script.py` | Core CLI pipeline orchestrator (email download -> extract -> process -> aggregate -> notification). |
| `downloademailonly.py` | Utility script to perform only the email download and extraction stages (no data transformations). |
| `testconnection.py` | Standalone diagnostic tool to verify Exchange server connection and `.env` credentials. |
| `exchange_lib.py` | EWS library handling NTLM authentication, mailbox folder navigation, email querying, attachment downloading, and outgoing emails. |
| `extract_zippy.py` | ZIP extraction utility with UTF-8 Windows console output and progress reporting. |
| `scheduler_db.py` | SQLite persistence layer managing `schedules` and `schedule_runs` in `schedules.db`. |
| `notification.py` | `Notifier` wrapper for sending success/error emails with attached logs. |
| `processing_3G_Ericsson.py` | Transforms multi-sheet 3G Ericsson XLSX files (`Throughput`, `Traffic_User`), calculating busy hour and traffic. |
| `processing_3G_ZTE.py` | Transforms 4 EMS1/EMS2 CSV files for 3G ZTE, merging traffic and user throughput on Cell Name. |
| `processing_4G_Ericsson.py` | Transforms multi-sheet 4G Ericsson XLSX files (`North_LTE_Traffic_Data`, `VoLTE_Traffic_Ericsson`). |
| `processing_4G_ZTE.py` | Transforms 4 EMS1/EMS2 CSV files for 4G ZTE, merging traffic and user throughput on Cell Name. |
| `schedules.db` | SQLite database storing scheduled jobs and execution history. |
| `SiteLocation.csv` | Optional reference file for mapping Cell IDs to Site coordinates. |
| `downloads/` | Temporary working directory for downloaded email attachments (auto-purged before runs). |
| `Log/` | Output directory storing per-day execution logs (`log_YYYY-MM-DD.txt`). |
| `requirements.txt` | Python dependencies. |
| `.env` | Environment configuration for Exchange credentials (not committed). |

---

## Data Transformation Pipeline

All 4 processors adhere to a standardized pipeline:

```
Raw Files (XLSX / CSV)
   └── 1. Import (load sheets/files)
   └── 2. Transform (busy-hour detection, traffic summation, speed lookup)
   └── 3. Standardize (rename to unified schema: [3G|4G]_Cell_ID, User, Speed, Voice, Data)
   └── 4. Clean (drop rows where Data == 0)
   └── 5. Aggregate (extract SiteID = Cell_ID[1:7]; sum User/Voice/Data, mean Speed)
```

### Standardized Site-Level Output Schema

Each processor outputs a DataFrame with the following standardized columns:

| Column Name | Aggregation Method | Description |
|---|---|---|
| `SiteID` | Extracted from `Cell_ID[1:7]` (characters 2 to 7) | Unique site identifier |
| `{3G\|4G}_User` | `sum()` across cells in site | Maximum simultaneous active users |
| `{3G\|4G}_Speed` | `mean()` across cells in site | Download throughput (Mbps) at busy hour |
| `{3G\|4G}_Voice` | `sum()` across cells in site | Voice traffic (Erlang) |
| `{3G\|4G}_Data` | `sum()` across cells in site | Total data traffic (MB) |

### Pandas 3.x Compatibility

To guard against runtime errors when finding busy hours on rows containing all-NA values, all processors employ `safe_idxmax_axis1()`:
```python
def safe_idxmax_axis1(df: pd.DataFrame) -> pd.Series:
    numeric = df.apply(pd.to_numeric, errors="coerce")
    result = pd.Series(pd.NA, index=numeric.index, dtype=object)
    valid = numeric.notna().any(axis=1)
    if valid.any():
        result.loc[valid] = numeric.loc[valid].idxmax(axis=1)
    return result
```

---

## Configuration & Environment Variables

Create a `.env` file in the project root directory with the following keys:

```ini
EXCHANGE_SERVER="mail.vietnamobile.com.vn"       # Exchange server hostname or EWS URL
EXCHANGE_DOMAIN="VNM"                           # Domain for NTLM authentication
EXCHANGE_USERNAME="username"                    # Domain username (without domain prefix)
EMAIL_PASSWORD="your_password"                  # Account password
EMAIL_ADDRESS="your_email@vietnamobile.com.vn"  # Primary SMTP email address
```

### Script-Level Settings (`script.py`)

- `PATH_TO_COPY`: Target directory to back up the generated `Aggregate.xlsx`.
- `RESULT_RECEIVER_LIST`: List of email addresses to receive run status and log attachments.
- `RESULT_EMAIL_SUBJECT`: Email subject prefix (default: `[Automate Job Result]`).
- `FOLDER_NAME` / `FOLDER_NAME_Z`: Mailbox folders to search (default: `"inbox"`).
- `SENDER_EMAIL` / `SENDER_EMAIL_Z`: Target sender filters for personal and shared mailboxes.

---

## Usage Guide

### 1. Web Interface (Recommended)

Start the Gradio dashboard for manual execution, job scheduling, and log monitoring:

```bash
python app_gradio.py
```
Open browser at `http://127.0.0.1:7860`.

- **Chạy báo cáo (Run Report):** Run ETL immediately, optionally selecting a specific date and toggle `--skip-email`.
- **Quản lý lịch (Schedule Management):** Add, view, pause, or delete recurring schedules (Daily/Weekly). Schedules are persisted in `schedules.db` and reloaded on startup.
- **Xem Log (View Logs):** View real-time log files and historical run statuses (OK / NOK).

### 2. Command Line Interface

#### Main Orchestrator (`script.py`)

```bash
# Run full pipeline for yesterday's data (default)
python script.py

# Process existing local files in downloads/ (skipping Exchange download)
python script.py --skip-email

# Run pipeline for a specific date (YYYY-MM-DD)
python script.py --process-date 2026-10-25

# Combine flags
python script.py --skip-email --process-date 2026-10-25
```

#### Diagnostic & Testing Tools

```bash
# Verify Exchange connection & credentials
python testconnection.py

# Download emails and extract archives only (no data transformation)
python downloademailonly.py
python downloademailonly.py --skip-email --process-date 2026-10-25

# Test individual processor modules directly (requires files in downloads/)
python processing_3G_Ericsson.py
python processing_3G_ZTE.py
python processing_4G_Ericsson.py
python processing_4G_ZTE.py
```

---

## Development Conventions & Operational Rules

1. **Exchange On-Premises Specifics:**
   - Authentication must use `auth_type=NTLM` and `access_type=DELEGATE`.
   - Never enable `autodiscover=True` because the Exchange endpoint is hardcoded in `EXCHANGE_SERVER`.
2. **Fresh Data Guarantee:**
   - `script.py` deliberately cleans the `downloads/` directory before downloading new files. Local file testing requires `--skip-email`.
3. **Chunked Excel Operations:**
   - `Aggregate.xlsx` accumulates ~180,000+ rows over 30 days. Never build entire workbooks in memory with `df.to_excel()`.
   - Always use `write_dataframe_to_excel_chunked()` (`openpyxl` `write_only=True`, 10k rows/chunk) with atomic `.tmp` replacement.
4. **Database Integrity:**
   - Do not alter `schedules.db` manually. Schema changes must go through migration logic inside `scheduler_db.py`.
5. **Cross-Platform Path Handling:**
   - Target OS is Windows. Use `pathlib.Path` or `os.path.join` for file path resolution.
   - Maintain UTF-8 stdout encoding for Windows console compatibility.

---

## Troubleshooting

| Issue | Root Cause | Solution |
|---|---|---|
| Exchange Connection Error | Incorrect credentials, VPN disconnected, or firewall blocking EWS port. | Run `python testconnection.py` to inspect connection handshake. Verify credentials in `.env`. |
| Missing Emails in Search | Date offset mismatch or subject typo. | Recall: Data for date **T** is sent on date **T+1**. Check `LIST_OF_EMAILS` and verify sender filter in `script.py`. |
| ZTE CSV File Not Found | Dynamic filename generated by ZTE OSS. | Ensure `extract_all_zips()` executed. Processors match files with wildcard glob patterns (`Automate_*.csv`). |
| Out of Memory on Excel Write | Loading entire 180k+ rows workbook into memory. | Ensure `write_dataframe_to_excel_chunked()` is used with `Workbook(write_only=True)`. |
| Scheduled Task Fails | Exchange credential expiry or network interruption. | Inspect `schedule_runs` in the Web UI history tab and review `Log/log_YYYY-MM-DD.txt`. |
