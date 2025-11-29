# Automated BI Report Generator

This project automates the process of generating a daily Business Intelligence (BI) report by processing telecommunications performance data from multiple vendors and technologies.

## Description

The script automatically downloads raw data from email attachments, processes it, and aggregates it into a single Excel report. It's designed to handle data from different vendors (Ericsson, ZTE) and technologies (3G, 4G), each with its own specific data format and transformation logic.

## Features

- **Automated Data Pipeline:** Fully automates the ETL (Extract, Transform, Load) process.
- **Email Integration:** Connects to a Microsoft Exchange server to find and download specific email attachments containing the raw data.
- **Multi-Vendor/Multi-Technology Support:** Includes dedicated processing modules for:
    - 3G Ericsson
    - 3G ZTE
    - 4G Ericsson
    - 4G ZTE
- **Complex Data Transformation:**
    - Performs 'busy hour' calculations for Ericsson data to identify key performance indicators.
    - Handles different file formats (Excel, CSV).
    - Standardizes column names across different data sources.
- **Data Aggregation:** Aggregates cell-level data to a site-level view.
- **Reporting:** Appends the daily processed data to a master Excel file, maintaining a rolling 30-day history.
- **Notifications:** Sends email notifications upon successful completion or failure of the process.

## Architecture & Workflow

1.  **Extract:** The main `script.py` initiates the process. It uses `exchange_lib.py` to connect to an MS Exchange server and download specific email attachments (zip files) into a `downloads` directory.
2.  **Unzip:** The downloaded `.zip` files are extracted using `extract_zippy.py`.
3.  **Transform:** Four distinct processing modules (`processing_*.py`) handle the core data transformation. They read the raw files, perform calculations (like 'busy hour' for Ericsson), standardize column names, and aggregate the data to the site level based on a common `SiteID`.
4.  **Load:** The main script merges the four processed DataFrames into a single master DataFrame. This is then enriched with location data from `SiteLocation.csv`.
5.  **Report:** The final, aggregated data is appended to `Aggregate.xlsx`. The report is pruned to keep only the last 30 days of data.
6.  **Notify:** `notification.py` sends a status email (success or failure) with logs to a predefined list of recipients.

## Setup and Configuration

1.  **Install Dependencies:** Install the required Python libraries using the `requirements.txt` file.
    ```bash
    pip install -r requirements.txt
    ```

2.  **Environment Variables:** Create a `.env` file in the root of the project directory to store sensitive information. This file should contain the credentials for the Exchange server and email notification settings.

    ```
    EXCHANGE_USERNAME="your_email@example.com"
    EXCHANGE_PASSWORD="your_password"
    EMAIL_RECIPIENTS="recipient1@example.com,recipient2@example.com"
    ```

3.  **Site Location Data:** Ensure you have a `SiteLocation.csv` file in the project root with site location information. It should contain at least `SiteID` and location columns.

## Usage

To run the entire automated process, execute the main script:

```bash
python script.py
```

## Project Structure

- `script.py`: The main entry point and orchestrator of the ETL pipeline.
- `requirements.txt`: A list of the Python libraries required for this project.
- `exchange_lib.py`: Module for interacting with the MS Exchange server (downloading attachments, sending emails).
- `extract_zippy.py`: Utility to unzip downloaded files.
- `notification.py`: Handles sending success or failure notification emails.
- `processing_3G_Ericsson.py`: Processor for 3G Ericsson data.
- `processing_3G_ZTE.py`: Processor for 3G ZTE data.
- `processing_4G_Ericsson.py`: Processor for 4G Ericsson data.
- `processing_4G_ZTE.py`: Processor for 4G ZTE data.
- `.env`: (To be created) Configuration file for credentials and other settings.
- `SiteLocation.csv`: (To be created) CSV file containing site location data.
- `downloads/`: Directory where email attachments are stored.
- `Aggregate.xlsx`: The final, aggregated Excel report.
