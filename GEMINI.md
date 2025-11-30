# Automate Report BI

## Project Overview
This project is an automated Business Intelligence (BI) report generator for Vietnamobile. It streamlines the processing of telecommunications network performance data (3G/4G) from Ericsson and ZTE vendors. The system automates the entire pipeline: downloading raw reports from Microsoft Exchange emails, extracting attachments (ZIP/Excel/CSV), transforming data (busy hour calculations, aggregation), and generating a final consolidated Excel report.

It features a user-friendly web interface built with **Gradio** for managing schedules, triggering manual runs, and monitoring logs.

## Key Features
*   **Automated ETL Pipeline:** Extracts data from emails, transforms it based on specific vendor logic, and loads it into a master report.
*   **Multi-Vendor Support:** Specialized processing modules for **Ericsson** (3G/4G) and **ZTE** (3G/4G).
*   **Web Interface (Gradio):** A local web dashboard to:
    *   Run tasks manually (full process or local files only).
    *   Schedule recurring jobs (Daily/Weekly).
    *   View execution logs.
*   **Persistent Scheduling:** Uses `APScheduler` backed by **SQLite** to ensure schedules survive application restarts.
*   **Email Integration:** Connects to Microsoft Exchange (via `exchangelib`) using NTLM authentication to fetch specific report emails.
*   **Notifications:** Sends email alerts with execution status and logs.

## Architecture & File Structure

### Entry Points
*   `app_gradio.py`: **Primary Entry Point.** Starts the Gradio web interface for user interaction and scheduling.
*   `script.py`: **Core Logic.** The main CLI script that orchestrates the download, extraction, and processing pipeline. Can be run standalone.

### Core Modules
*   `exchange_lib.py`: Handles connection to MS Exchange, folder navigation, and email downloading.
*   `scheduler_db.py`: Manages the SQLite database (`schedules.db`) for storing job schedules and run history.
*   `extract_zippy.py`: Utility for finding and extracting ZIP files in the download directory.
*   `notification.py`: Handles sending status emails after job completion.

### Data Processing
*   `processing_3G_Ericsson.py`: Logic for Ericsson 3G data (Throughput, Traffic, VoLTE).
*   `processing_3G_ZTE.py`: Logic for ZTE 3G data (Traffic, User Throughput).
*   `processing_4G_Ericsson.py`: Logic for Ericsson 4G data.
*   `processing_4G_ZTE.py`: Logic for ZTE 4G data.

### Configuration & Data
*   `requirements.txt`: Python dependencies (`pandas`, `gradio`, `exchangelib`, `apscheduler`, etc.).
*   `.env`: Stores sensitive credentials (Exchange username/password). **Do not commit.**
*   `SiteLocation.csv`: Required reference file for mapping Cell IDs to Sites.
*   `downloads/`: Temporary storage for downloaded email attachments.
*   `Log/`: Stores execution logs.

## Setup & Installation

1.  **Environment:**
    Ensure Python 3.8+ is installed.

2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configuration:**
    Create a `.env` file in the root directory with the following keys:
    ```ini
    EXCHANGE_USERNAME="your_email@vietnamobile.com.vn"
    EXCHANGE_PASSWORD="your_password"
    EMAIL_RECIPIENTS="recipient1@example.com,recipient2@example.com"
    # Add other keys as required by script.py
    ```
    Ensure `SiteLocation.csv` is present in the root directory.

## Usage

### Running the Web Interface (Recommended)
To start the dashboard for scheduling and monitoring:
```bash
python app_gradio.py
```
Access the UI at the URL displayed in the terminal (usually `http://127.0.0.1:7860`).

### Running via CLI
To execute the pipeline manually without the UI:

*   **Full Run (Download + Process):**
    ```bash
    python script.py
    ```

*   **Process Local Files Only (Skip Download):**
    ```bash
    python script.py --skip-email
    ```

## Development Conventions
*   **Modular Processing:** New data sources should have their own `processing_*.py` module following the existing pattern (Import -> Transform -> Standardize -> Aggregate).
*   **Database:** All scheduling persistence logic resides in `scheduler_db.py`. Do not modify the DB schema without updating this file.
*   **Logging:** Use the `Logger` class (configured in `script.py`) to ensure output is captured in both the console and log files in the `Log/` directory.
*   **Path Handling:** Use `os.path.join` or `pathlib` for cross-platform compatibility (though currently targeted for Windows).

## Troubleshooting
*   **Exchange Connection:** Run `python testconnection.py` to verify NTLM authentication and server connectivity.
*   **Logs:** Check the `Log/` directory for detailed execution traces if a job fails. The web UI also provides a log viewer.
