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
  - Executes both email search passes
  - Defines all search criteria (subjects, senders, folders)

- **`exchange_lib.py`** - Core library with three main functions:
  - `get_exchange_account()` - Establishes Exchange connection using NTLM authentication
  - `find_subfolder()` - Navigates mailbox folder hierarchy
  - `find_and_download_emails()` - Searches emails by criteria (date range, subject, sender, folder) and downloads attachments

- **`testconnection.py`** - Standalone utility for validating Exchange connectivity

### Data Flow

```
script.py (config & orchestration)
  └─> exchange_lib functions
        ├─> get_exchange_account() → Exchange connection
        ├─> find_subfolder() → Locate target folder
        └─> find_and_download_emails() → Search & download
              └─> downloads/ (output directory)
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
- Standard library modules: `os`, `logging`, `shutil`, `datetime`

## Important Development Notes

- **NTLM Authentication**: This solution is specific to on-premises Exchange servers with NTLM authentication. It uses `exchangelib` library which handles the protocol details.
- **Folder Cleanup**: The application clears the entire download folder before each run to ensure fresh data. This is intentional per recent refactoring (commit: ba4dc41).
- **Code Organization**: Previous refactoring (commit: 248135b) moved email checking and retrieval logic into the library module to separate concerns between orchestration (script.py) and Exchange operations (exchange_lib.py).
- **Search Strategy**: Emails are filtered by date range, subject keywords, and sender. The two-pass approach handles both personal mailbox and shared mailbox scenarios with different search criteria for each.
- **Attachment Filtering**: Downloaded files are filtered by extension during the download process.

## Logging

Console output includes emoji indicators (✅ for success, ❌ for errors, ℹ️ for info). Logs can be written to the `Log/` directory if configured in the logging setup.

## Recent Changes

- Folder cleanup before processing (ensures fresh data)
- Code refactoring to move Exchange operations to library module
- Single file download verification
- Exchange server connection validation

