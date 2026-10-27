# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ETL automation for Vietnamobile network KPI reporting. It connects to an on-premises Microsoft Exchange server over EWS/NTLM, downloads 3G/4G Ericsson and ZTE report attachments for a single processing date, extracts ZIPs, transforms busy-hour/traffic metrics, aggregates cell data to site level, and appends the result to one PowerBI source file (`Aggregate.xlsx`). A Gradio web UI adds scheduling and log viewing. UI labels, README, and most runtime messages are in Vietnamese.

## Commands

```bash
pip install -r requirements.txt

# Full pipeline (download + extract + process), defaults to yesterday's data
python script.py

# Process files already present in downloads/ (no Exchange connection)
python script.py --skip-email

# Process a specific data date
python script.py --process-date 2026-10-25
python script.py --skip-email --process-date 2026-10-25

# Backfill several dates (comma-separated, run sequentially; failing dates are
# bypassed and the loop continues). Exit code 0 = all OK, 1 = some date failed,
# 2 = invalid --process-date.
python script.py --process-date 2026-10-01,2026-10-02,2026-10-03

# Download + extract only (no data processing)
python downloademailonly.py [--skip-email] [--process-date YYYY-MM-DD[,YYYY-MM-DD,...]]

# Validate .env / Exchange connectivity
python testconnection.py

# Web dashboard + scheduler (http://127.0.0.1:7860)
python app_gradio.py

# Per-processor smoke test against files in downloads/
python processing_3G_Ericsson.py
python processing_3G_ZTE.py
python processing_4G_Ericsson.py
python processing_4G_ZTE.py
```

There is no test suite, linter, or build step. Validation is running the scripts above. `script.py --skip-email` is the fastest end-to-end check when `downloads/` is already populated.

## Working Directory and Outputs

All runtime paths are relative to the **current working directory**, not the script file: `downloads/`, `Log/`, `schedules.db`. Launch everything from the repository root. `app_gradio.py` spawns `script.py` with the inherited CWD.

- Final report: `Aggregate.xlsx` in the **parent** of this project directory (`Path(__file__).parent.parent`).
- Optional backup copy: `PATH_TO_COPY` in `script.py` (currently `.../Backup_Aggregate/`). A copy failure is logged but does not fail the run.
- Logs: `Log/log_<process_date>.txt`.
- Downloads: `downloads/` is deleted and recreated on every non-`--skip-email` run.

## Pipeline Architecture

```
app_gradio.py ──subprocess──> script.py ──> exchange_lib.py (EWS/NTLM)
                                  ├──────> extract_zippy.py
                                  ├──────> processing_*.py (4 processors)
                                  ├──────> notification.py (success/failure email + log)
                                  └──────> ../Aggregate.xlsx
```

`script.py` is the orchestrator; all tunable constants live in its top "CẤU HÌNH" block. It:

1. Parses `--skip-email` / `--process-date` via `cli_utils` (`build_arg_parser`, `parse_date_list`), then derives `process_date` (default yesterday) and `email_search_date = process_date + 1 day`. `--process-date` accepts a comma-separated list; `main()` runs dates sequentially and bypasses a failing date.
2. Connects with `get_exchange_account()` (EWS `NTLM`, `DELEGATE`, `autodiscover=False`) **once**, before the date loop.
3. Clears/recreates `downloads/`.
4. Downloads **Pass 1 (Ericsson)**: folder `inbox`, sender `SENDER_EMAIL`, subjects in `LIST_OF_EMAILS` (4 XLSX reports).
5. Downloads **Pass 2 (ZTE)**: folder `inbox`, sender `SENDER_EMAIL_Z`, subjects in `LIST_OF_EMAILS_Z` (8 `[EXTERNAL]Task name:...` reports delivered as ZIPs).
6. Extracts every ZIP in `downloads/`.
7. Runs the four processors, concatenates Ericsson + ZTE per technology, outer-merges 3G and 4G on `SiteID`, fills missing with 0, adds `Date`, renames to the final PowerBI schema.
8. `merge_into_aggregate()` deletes rows for that exact `process_date` (idempotent re-runs), appends, keeps the last `RETENTION_DAYS` (30) days compared by **normalized date**, sorts by `Date`, writes atomically.
9. Copies to `PATH_TO_COPY` (if set) and emails success/failure via `Notifier`; mail errors are logged and never change the run result.

Exit codes: `0` all dates OK, `1` a date failed or Exchange connect failed, `2` invalid `--process-date`. `main()` is called as `sys.exit(main())`; `app_gradio.py` maps these codes to `OK` / `NOK`.

Both passes query the same `inbox`; they are distinguished only by sender and exact subject match. Email search is by `datetime_received` within one local day (`Asia/Ho_Chi_Minh`) and `subject` exact-equals the configured string — subject changes in Exchange require updating the lists in `script.py`.

**Date rule:** data for day **T** arrives in an email sent on **T+1**. `--process-date` is the data date, so the email search uses `process_date + 1 day`. `DAYS_TO_SEARCH` is effectively bypassed because `script.py` always passes `target_date`.

## Configuration

`.env` keys (never committed): `EMAIL_ADDRESS`, `EMAIL_PASSWORD`, `EXCHANGE_SERVER`, `EXCHANGE_DOMAIN`, `EXCHANGE_USERNAME`. The NTLM username is sent as `DOMAIN\USERNAME`.

Constants in `script.py` that are commonly edited: `LIST_OF_EMAILS` / `LIST_OF_EMAILS_Z`, `SENDER_EMAIL` / `SENDER_EMAIL_Z`, `FOLDER_NAME` / `FOLDER_NAME_Z`, `DOWNLOAD_FOLDER`, `PATH_TO_COPY`, `ALLOWED_EXTENSIONS`, `DAYS_TO_SEARCH`, `RETENTION_DAYS`, `LOG_LEVEL`, `RESULT_RECEIVER_LIST`, `RESULT_EMAIL_SUBJECT`.

## Processor Contract

Each processor (`Ericsson3GProcessor`, `ZTE3GProcessor`, `Ericsson4GProcessor`, `ZTE4GProcessor`) follows the same call sequence used by `script.py`:

```
<load>_data() → merge_final_result() → standardize_columns() → clean_data() → aggregate_by_site()
```

Loader names differ: `load_all_3g_data` (3G Ericsson, also calls `transform_all()` before merge), `load_all_3g_zte_data`, `load_all_4g_ericsson_data`, `load_all_4g_zte_data`. Ericsson input is multi-sheet XLSX; ZTE input is 4 CSVs per technology found by glob because filenames carry dynamic suffixes (`Automate_{3G,4G}_ZTE_{Traffic_EMS1/2_WD,User_TP_EMS1/2_BH}_*.csv`).

Common rules:

- Standardized cell columns: `{3G|4G}_Cell_ID, {3G|4G}_User, {3G|4G}_Speed, {3G|4G}_Voice, {3G|4G}_Data`.
- `clean_data()` drops rows where `{3G|4G}_Data == 0`.
- `aggregate_by_site()` extracts `SiteID = Cell_ID[1:7]` (6 characters), then **sums** User/Voice/Data and **averages** Speed.
- Each processor stores intermediate/final frames in `self.dataframes[...]` for inspection.
- Ericsson busy-hour logic locates the max-user column per row; use `safe_idxmax_axis1()` (already defined) instead of `DataFrame.idxmax(axis=1)` so all-NA rows do not break under pandas 3.x.

Final `Aggregate.xlsx` columns:

```
Date, Site, 3G Sub, 3G Speed, 3G Voice traffic, 3G Data traffic,
4G Sub, 4G Speed, 4G Voice traffic, 4G Data traffic
```

## Excel Writing

Appending can produce ~180k rows, so `script.py` uses `write_dataframe_to_excel_chunked()` instead of `DataFrame.to_excel()`: openpyxl `Workbook(write_only=True)` in 10k-row chunks, `_excel_cell_value()` normalizes numpy/pandas/NaN values, and the workbook is saved to `.tmp` then `os.replace()`d for an atomic update. Preserve this path for any change touching `Aggregate.xlsx`; `to_excel()` risks memory blowups and file corruption on interruption.

## Web UI and Scheduling

`app_gradio.py` starts a `BackgroundScheduler` (`Asia/Ho_Chi_Minh`) and, on startup, calls `sync_scheduler_from_db()` to re-register active schedules from `schedules.db`. Job IDs are `db_job_<schedule_id>`.

- Manual runs use `subprocess.Popen` (fire-and-forget); the UI only reports that the process started, so check the Logs tab.
- Scheduled runs use `subprocess.run` with captured output and record `OK`/`NOK` plus stderr tail via `scheduler_db.log_run()`. Scheduled runs do not pass `--process-date`, so they process yesterday by default.
- Schedule rows store `script_path`, so any `.py` in the project can be scheduled, not just `script.py`.

`scheduler_db.py` owns all SQLite access (`schedules`, `schedule_runs`). `init_db()` runs on import and performs the `script_path` column migration. Do not alter the schema outside this module.

## Logging and Notifications

`script.py`/`downloademailonly.py` replace `sys.stdout`/`sys.stderr` with the `Logger` class, duplicating output to the console and `Log/log_<process_date>.txt` (flushed per write). `notification.Notifier` sends a success or failure email with that log attached; on failure before a connection exists, `script.py` retries `get_exchange_account()` once just to send the alert. Console output uses emoji status markers (✅/❌/⚠️/📥).

## Notes and Gotchas

- Exchange integration is specific to on-premises EWS with NTLM delegate access; do not switch to autodiscover.
- `processing_4G_Ericsson` also reads `Automate_VoLTE_Traffic_Ericsson.xlsx`; all four Ericsson inputs must be present or the processor raises.
- `SiteLocation.csv` and the Long/Lat lookup are currently commented out in `script.py`; location is handled elsewhere. Do not assume location columns exist.
- `app_gradio.py` executes Gradio UI construction at import time (module-level `with gr.Blocks(...)`) and starts the scheduler at import; it is not import-safe for testing.
- The project targets Windows; paths are built with `pathlib`/`os.path` but backslash literals appear in configuration examples.
- `.trae/rules/project_rules.md` defines a 5-step workflow for large requests (analyze/plan, execute, test plan, debug, report). Follow it for substantial changes in this repository.
