# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python automation tool that downloads PowerBI performance reports from Microsoft Exchange servers for Vietnamobile. It connects to on-premises Exchange servers using NTLM authentication, searches for emails matching specific criteria (subject lines, senders, date ranges), and downloads attachments to a local directory.

## Common Commands

```bash
# Run the main automation script
python script.py

# Test Exchange server connection (without downloading)
python testconnection.py
```

## Architecture Overview

### Two-Pass Email Processing

The application uses a two-pass strategy to collect reports from different email sources:

1. **Pass 1 (Personal Mailbox)**: Downloads reports from the user's own mailbox
   - Folder: "Myself"
   - Sender: User's email address
   - Subjects: 3G_Throughput, 3G_Traffic_User, VoLTE_Traffic_Ericsson, North_LTE_Traffic_Data

2. **Pass 2 (Shared Mailbox)**: Downloads reports from a shared performance reporting mailbox
   - Folder: "inbox" (in shared account)
   - Sender: vnm.performance.reporting@vietnamobile.com.vn
   - Subjects: [EXTERNAL]Task name:* (covers 3G/4G ZTE traffic variants)

### Core Components

- **`script.py`** - Main entry point that orchestrates the workflow:
  - Loads credentials from `.env` file
  - Clears the download folder before processing
  - Executes both email search passes (personal + shared mailbox)
  - Defines all search criteria (subjects, senders, folders)
  - Triggers automatic ZIP extraction after downloads complete

- **`exchange_lib.py`** - Core library with three main functions:
  - `get_exchange_account()` - Establishes Exchange connection using NTLM authentication
  - `find_subfolder()` - Navigates mailbox folder hierarchy
  - `find_and_download_emails()` - Searches emails by criteria (date range, subject, sender, folder) and downloads attachments

- **`extract_zippy.py`** - ZIP file extraction module:
  - `extract_all_zips()` - Finds and extracts all ZIP files in download folder
  - `extract_and_cleanup_zips()` - Extracts and optionally deletes ZIP files after extraction
  - UTF-8 encoding support for Windows console
  - Detailed progress reporting with emoji indicators

- **`testconnection.py`** - Standalone utility for validating Exchange connectivity

### Data Processing Modules

All processing modules follow a standardized 4-step workflow:
1. **Import** - Load data from Excel/CSV files
2. **Transform** - Calculate metrics (max users, traffic sums, speeds)
3. **Merge** - Combine multiple sheets/files into single DataFrame
4. **Standardize** - Normalize column names to common format
5. **Clean** - Remove invalid data (rows with Data = 0)
6. **Aggregate** - Roll up cell-level data to site-level by SiteID

#### 3G Ericsson Processor

- **`processing_3G_Ericsson.py`** - `Ericsson3GProcessor` class
  - **Input Files:**
    - `Automate_3G_Throughput.xlsx` (User_TP_DL sheet)
    - `Automate_3G_Traffic_User.xlsx` (Voice_Erlang, Data_MB, HS_User sheets)
  - **Transformations:**
    - Extract max users and busy hour index from HS_User
    - Calculate voice/data traffic sums
    - Lookup 3G speed at busy hour
  - **Cell-Level Output:** `Final_3G_Result` (Ucell ID, Max_user, 3G_Speed, Voice, Data)
  - **Standardized:** `3G_Cell_ID, 3G_User, 3G_Speed, 3G_Voice, 3G_Data`
  - **Site-Level Output:** `3G_Ericsson_Site_Data` (SiteID + aggregated metrics)

#### 3G ZTE Processor

- **`processing_3G_ZTE.py`** - `ZTE3GProcessor` class
  - **Input Files:** 4 CSV files with pattern matching
    - `Automate_3G_ZTE_Traffic_EMS1_WD_*.csv`
    - `Automate_3G_ZTE_Traffic_EMS2_WD_*.csv`
    - `Automate_3G_ZTE_User_TP_EMS1_BH_*.csv`
    - `Automate_3G_ZTE_User_TP_EMS2_BH_*.csv`
  - **Processing:**
    - Concatenate EMS1 + EMS2 traffic data
    - Concatenate EMS1 + EMS2 user throughput data
    - Merge on Cell Name
  - **Cell-Level Output:** `3G_ZTE_DATA` (Cell Name, Average HSDPA Users, User DL Throughput, AMR Traffic, Total Data Traffic)
  - **Standardized:** `3G_Cell_ID, 3G_User, 3G_Speed, 3G_Voice, 3G_Data`
  - **Site-Level Output:** `3G_ZTE_Site_Data` (SiteID + aggregated metrics)

#### 4G ZTE Processor

- **`processing_4G_ZTE.py`** - `ZTE4GProcessor` class
  - **Input Files:** 4 CSV files with pattern matching
    - `Automate_4G_ZTE_Traffic_EMS1_WD_*.csv`
    - `Automate_4G_ZTE_Traffic_EMS2_WD_*.csv`
    - `Automate_4G_ZTE_User_TP_EMS1_BH_*.csv`
    - `Automate_4G_ZTE_User_TP_EMS2_BH_*.csv`
  - **Processing:**
    - Concatenate EMS1 + EMS2 traffic data
    - Concatenate EMS1 + EMS2 user throughput data
    - Merge on Cell Name
  - **Cell-Level Output:** `4G_ZTE_DATA` (Cell Name, Average DL Active User Number, DL_THP_PER_USER, LTE QCI1 Traffic, Data)
  - **Standardized:** `4G_Cell_ID, 4G_User, 4G_Speed, 4G_Voice, 4G_Data`
  - **Site-Level Output:** `4G_ZTE_Site_Data` (SiteID + aggregated metrics)

#### 4G Ericsson Processor

- **`processing_4G_Ericsson.py`** - `Ericsson4GProcessor` class
  - **Input Files:**
    - `Automate_North_LTE_Traffic_Data.xlsx` (Data_MB, UE_Active_DL, UE_TP_DL sheets)
    - `Automate_VoLTE_Traffic_Ericsson.xlsx` (VoLTE_Traffic sheet)
  - **Transformations:**
    - Calculate total data traffic (sum of 24 hours)
    - Extract max UE active and busy hour index
    - Lookup throughput at max UE hour
    - Calculate VoLTE traffic sum
  - **Cell-Level Output:** `4G_ERICSSON_DATA` (Cell ID, max_UE_Active, throughput_max_UE_Active, traffic_VoLTE_4G, traffic_data_4G)
  - **Standardized:** `4G_Cell_ID, 4G_User, 4G_Speed, 4G_Voice, 4G_Data`
  - **Site-Level Output:** `4G_Ericsson_Site_Data` (SiteID + aggregated metrics)

#### Common Processing Steps

All processors implement these methods:
- `standardize_columns()` - Normalize column names to common format (3G/4G_Cell_ID, User, Speed, Voice, Data)
- `clean_data()` - Remove rows where Data column = 0
- `aggregate_by_site()` - Extract SiteID from Cell_ID (characters 1-6) and aggregate:
  - **SiteID** - Extracted from Cell_ID[1:7]
  - **User** - Sum of all cells in site
  - **Speed** - Average speed across cells
  - **Voice** - Sum of voice traffic
  - **Data** - Sum of data traffic

### Data Flow

```
script.py (config & orchestration)
  └─> exchange_lib functions
        ├─> get_exchange_account() → Exchange connection
        ├─> find_subfolder() → Locate target folder
        └─> find_and_download_emails() → Search & download
              └─> downloads/ (XLSX + ZIP files)
                    └─> extract_zippy.extract_all_zips() → Extract ZIPs
                          └─> downloads/ (XLSX + CSV files)
                                └─> Data Processing Modules
                                      ├─> processing_3G_Ericsson.Ericsson3GProcessor
                                      ├─> processing_3G_ZTE.ZTE3GProcessor
                                      ├─> processing_4G_ZTE.ZTE4GProcessor
                                      └─> processing_4G_Ericsson (TBD)
                                            └─> Processed DataFrames for PowerBI
```

## Configuration

Environment variables (stored in `.env`, never committed):
- `EMAIL_ADDRESS` - User's Exchange email address
- `EMAIL_PASSWORD` - User's password
- `EXCHANGE_SERVER` - Exchange server hostname
- `EXCHANGE_DOMAIN` - Domain for NTLM authentication
- `EXCHANGE_USERNAME` - Domain username for NTLM

Additional configuration in `script.py`:
- `SEARCH_SUBJECTS` - List of email subject keywords to filter on
- `SHARED_EMAIL_ADDRESS` - Shared mailbox email for second pass
- `SHARED_SENDER_EMAIL` - Email address to filter in shared mailbox
- `SHARED_SEARCH_SUBJECTS` - Subject filters for shared mailbox
- `START_DATE` and `END_DATE` - Email search date range
- `DOWNLOAD_FOLDER` - Local directory for downloaded files

## Key Dependencies

- `exchangelib` - Microsoft Exchange Web Services (EWS) client
- `python-dotenv` - Environment variable management
- `pandas` - Data processing and transformation
- Standard library modules: `os`, `logging`, `shutil`, `datetime`, `zipfile`, `pathlib`, `glob`

## Important Development Notes

- **NTLM Authentication**: This solution is specific to on-premises Exchange servers with NTLM authentication. It uses `exchangelib` library which handles the protocol details.
- **Folder Cleanup**: The application clears the entire download folder before each run to ensure fresh data. This is intentional per recent refactoring (commit: ba4dc41).
- **Code Organization**: Previous refactoring (commit: 248135b) moved email checking and retrieval logic into the library module to separate concerns between orchestration (script.py) and Exchange operations (exchange_lib.py).
- **Search Strategy**: Emails are filtered by date range, subject keywords, and sender. The two-pass approach handles both personal mailbox and shared mailbox scenarios with different search criteria for each.
- **Attachment Filtering**: Downloaded files are filtered by extension during the download process.
- **ZIP Extraction**: After downloading attachments, the script automatically extracts all ZIP files in the downloads folder to access CSV data files.
- **Data Processing**: Modular processors for each vendor/technology (3G/4G Ericsson/ZTE) handle data transformation independently.
- **Pattern Matching**: ZTE processors use glob pattern matching to find CSV files with dynamic suffixes.

## Logging

Console output includes emoji indicators (✅ for success, ❌ for errors, ℹ️ for info). Logs can be written to the `Log/` directory if configured in the logging setup.

## Recent Changes

- **Logging & Notification System:**
  - Implemented `Logger` class to capture console output to file
  - Created `notification.py` module for automated email reporting
  - Added `send_email` capability to `exchange_lib.py`
  - Integrated error handling in `script.py` to catch failures and send alerts
  - Configured automatic success/failure emails with attached logs
- **Data Processing Pipeline (Complete):**
  - Added `processing_3G_Ericsson.py` - Process 3G Ericsson XLSX files with transformation logic
  - Added `processing_3G_ZTE.py` - Process 3G ZTE CSV files with pattern matching
  - Added `processing_4G_ZTE.py` - Process 4G ZTE CSV files with pattern matching
  - Added `processing_4G_Ericsson.py` - Process 4G Ericsson XLSX files with multi-sheet handling
  - Implemented standardization of column names across all processors
  - Added data cleaning step (remove rows with Data = 0)
  - Implemented site-level aggregation (extract SiteID and aggregate metrics)
- Added `extract_zippy.py` module for automatic ZIP file extraction
- Integrated ZIP extraction into main workflow (script.py)
- Updated email subject lists for ZTE traffic reports (EMS1 and EMS2 variants)
- Folder cleanup before processing (ensures fresh data)
- Code refactoring to move Exchange operations to library module
- Single file download verification
- Exchange server connection validation

## Workflow Summary

1. **Connect** to Exchange server (NTLM authentication)
2. **Clean** downloads folder
3. **Download Pass 1** - Personal mailbox (4 XLSX files: Throughput, Traffic User, VoLTE, North LTE)
4. **Download Pass 2** - Shared mailbox (8 ZIP files: 3G/4G ZTE Traffic + User TP for EMS1/EMS2)
5. **Extract** all ZIP files automatically (8 CSV files extracted)
6. **Process** data files:
   - **3G Ericsson:** 
     - Import 2 XLSX files (4 sheets total)
     - Transform: max users, busy hour, traffic sums, speed lookup
     - Standardize → Clean → Aggregate
     - Output: `3G_Ericsson_Site_Data` (2,172 sites from 8,582 cells)
   - **3G ZTE:**
     - Import 4 CSV files (EMS1/2 Traffic + User TP)
     - Concatenate EMS1 + EMS2 data
     - Merge traffic + user throughput
     - Standardize → Clean → Aggregate
     - Output: `3G_ZTE_Site_Data` (3,707 sites from 12,770 cells)
   - **4G ZTE:**
     - Import 4 CSV files (EMS1/2 Traffic + User TP)
     - Concatenate EMS1 + EMS2 data
     - Merge traffic + user throughput
     - Standardize → Clean → Aggregate
     - Output: `4G_ZTE_Site_Data` (SiteID + aggregated metrics)
   - **4G Ericsson:**
     - Import 2 XLSX files (4 sheets total)
     - Transform: data traffic, max UE, throughput lookup, VoLTE traffic
     - Standardize → Clean → Aggregate
     - Output: `4G_Ericsson_Site_Data` (SiteID + aggregated metrics)
7. **Result** - Site-level DataFrames ready for PowerBI import with standardized schema:
   - `SiteID, User, Speed, Voice, Data` (consistent across all 4 processors)
