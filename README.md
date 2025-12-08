# Automated BI Report Generator

This project automates the process of generating a daily Business Intelligence (BI) report by processing telecommunications performance data. It provides a comprehensive web interface for easy management, execution, and monitoring of the data processing tasks.

## Web Interface

The primary way to interact with this application is through a web interface built with Gradio. To run it, execute:

```bash
python app_gradio.py
```

The interface provides the following functionalities:

- **▶️ Chạy thủ công (Manual Run):** Immediately trigger the data processing pipeline. You can now select a specific Python script (`.py` file) to run. You can choose to run the full process (including downloading from email) or to run only the local file processing part.
- **📅 Lịch chạy (Scheduler):**
<<<<<<< HEAD
    - **Thêm mới lịch chạy (Add New Schedule)::** Create new automated jobs. You can define the job name, frequency (daily/weekly), run time, execution mode (full or skip email), and crucially, **select the Python script to be executed for this schedule**.
    - **Quản lý lịch chạy đã có (Manage Existing Schedules):** View all saved schedules, including the assigned script path for each. You can activate, deactivate, delete, or view the execution history for any schedule. The history shows the timestamp, status (OK/NOK), and details for each run.
=======
  - **Thêm mới lịch chạy (Add New Schedule):** Create new automated jobs. You can define the job name, frequency (daily/weekly), run time, and execution mode (full or skip email).
    - **Quản lý lịch chạy đã có (Manage Existing Schedules):** View all saved schedules. You can activate, deactivate, delete, or view the execution history for any schedule. The history shows the timestamp, status (OK/NOK), and details for each run.
>>>>>>> 8d811d31a00dda35af7afe62a1f23b8eb225f599
- **📄 Xem Logs (View Logs):** View the detailed logs from `script.py` for troubleshooting and monitoring. You can select log files from a dropdown and refresh the list.
- **☎️ Liên hệ (Contact):** Provides contact information for support.

## Core Features

- **Web-Based Management:** A user-friendly Gradio interface to control and monitor all aspects of the application.
- **Flexible Task Scheduling:** Powered by APScheduler, allowing for daily or weekly jobs to be scheduled at specific times.
- **Persistent Schedules:** Schedules are saved in a local SQLite database (`schedules.db`), so they persist even if the application is restarted.
- **Run History Logging:** Every execution of a scheduled job is logged to the database, recording the time, status (OK/NOK), and any error details.
- **Automated Data Pipeline:** Fully automates the ETL (Extract, Transform, Load) process.
- **Email Integration:** Connects to a Microsoft Exchange server to find and download specific email attachments containing raw data.
- **Multi-Vendor/Multi-Technology Support:** Includes dedicated processing modules for 3G/4G data from Ericsson and ZTE.
- **Complex Data Transformation:** Performs 'busy hour' calculations, handles various file formats, and standardizes data.
- **Reporting & Notifications:** Aggregates data into a master Excel file and sends status notifications via email.

## Setup and Configuration

1. **Install Dependencies:** Install the required Python libraries using the `requirements.txt` file.

    ```bash
    pip install -r requirements.txt
    ```

2. **Environment Variables:** Create a `.env` file in the root of the project to store credentials for the Exchange server and email notifications.

    ```bash
    EXCHANGE_USERNAME="your_email@example.com"
    EXCHANGE_PASSWORD="your_password"
    EMAIL_RECIPIENTS="recipient1@example.com,recipient2@example.com"
    ```

3. **Site Location Data:** Ensure you have a `SiteLocation.csv` file in the project root with site location information.

## Usage

### 1. Running the Web Interface (Recommended)

Launch the Gradio application:

```bash
python app_gradio.py
```

Then open your web browser and navigate to the local URL provided (usually `http://127.0.0.1:7860`). Use the interface to manage schedules and run tasks.

**New Script Selection Feature:**
-   **Manual Run Tab:** On the "▶️ Chạy thủ công" tab, you will find a dropdown menu to select the Python script (`.py` file) you wish to execute. By default, it will be `script.py`. You can choose any other `.py` file present in the project's root directory.
-   **Scheduler Tab:** When adding a new schedule ("Thêm mới lịch chạy"), a similar dropdown is available to specify which script should be run for that particular scheduled job. The chosen script will be stored with the schedule and displayed in the "Quản lý lịch chạy đã có" table.

### 2. Running the Script Manually

You can still run the entire ETL process directly from the command line:

```bash
# Run the full process with default script.py
python script.py

# Run a specific script with the full process
python your_custom_script.py

# Run the default script.py but skip the email download part
python script.py --skip-email

# Run a specific script but skip the email download part
python your_custom_script.py --skip-email
```

## Project Structure

- `app_gradio.py`: The main entry point for the Gradio web interface.
- `script.py`: The core orchestrator of the ETL pipeline.
- `scheduler_db.py`: Module for all database interactions (creating tables, adding/deleting/querying schedules and run history).
- `schedules.db`: The SQLite database file that stores all schedules and run logs.
- `requirements.txt`: A list of the Python libraries required for this project.
- `exchange_lib.py`: Module for interacting with the MS Exchange server.
- `extract_zippy.py`: Utility to unzip downloaded files.
- `notification.py`: Handles sending notification emails.
- `processing_*.py`: Modules for specific data transformations (3G/4G, Ericsson/ZTE).
- `downloads/`: Directory where email attachments are stored.
- `Log/`: Directory containing detailed run logs from `script.py`.
- `Aggregate.xlsx`: The final, aggregated Excel report.
