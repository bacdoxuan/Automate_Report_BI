
import os
import sys
import logging
import math
import numpy as np
import pandas as pd
from notification import Notifier
from datetime import datetime, timedelta, date
from pathlib import Path
from dotenv import load_dotenv
from exchange_lib import get_exchange_account, find_and_download_emails
from extract_zippy import extract_all_zips
from processing_3G_Ericsson import Ericsson3GProcessor
from processing_3G_ZTE import ZTE3GProcessor
from processing_4G_Ericsson import Ericsson4GProcessor
from processing_4G_ZTE import ZTE4GProcessor
from openpyxl import Workbook
import shutil
import argparse

# Load biến môi trường từ .env
load_dotenv()

# =================================================================
# ========== CẤU HÌNH - CÓ THỂ THAY ĐỔI Ở ĐÂY =====================
# =================================================================

# Thư mục tìm kiếm (tên chính xác trong Exchange)
FOLDER_NAME = "inbox"
FOLDER_NAME_Z = "inbox"

# Email người gửi (None = không lọc theo người gửi)
SENDER_EMAIL = "bac.dx@vietnamobile.com.vn"
SENDER_EMAIL_Z = "vnm.performance.reporting@vietnamobile.com.vn"

# Danh sách tiêu đề email cần tìm
LIST_OF_EMAILS = [
    "Automate_3G_Throughput",
    "Automate_3G_Traffic_User",
    "Automate_VoLTE_Traffic_Ericsson",
    "Automate_North_LTE_Traffic_Data",
    # Thêm các tiêu đề email khác vào đây
]

LIST_OF_EMAILS_Z = [
    "[EXTERNAL]Task name:Automate_3G_ZTE_Traffic_EMS1_WD",
    "[EXTERNAL]Task name:Automate_3G_ZTE_User_TP_EMS1_BH",
    "[EXTERNAL]Task name:Automate_4G_ZTE_Traffic_EMS1_WD",
    "[EXTERNAL]Task name:Automate_4G_ZTE_User_TP_EMS1_BH",
    "[EXTERNAL]Task name:Automate_3G_ZTE_Traffic_EMS2_WD",
    "[EXTERNAL]Task name:Automate_3G_ZTE_User_TP_EMS2_BH",
    "[EXTERNAL]Task name:Automate_4G_ZTE_Traffic_EMS2_WD",
    "[EXTERNAL]Task name:Automate_4G_ZTE_User_TP_EMS2_BH",
    # Thêm các tiêu đề email khác vào đây
]

# Thư mục lưu file tải về
DOWNLOAD_FOLDER = "downloads"

# Thư mục sao chép file kết quả (để trống = không sao chép)
# Ví dụ: PATH_TO_COPY = r"D:\Reports\Archive"
PATH_TO_COPY = r"D:/Project/Automate PowerBI - Display Site Information/Backup_Aggregate/"

# Chỉ download các định dạng file này (để trống = tất cả file)
# Ví dụ: [".xlsx", ".pdf", ".csv"]
ALLOWED_EXTENSIONS = []

# Thời gian tìm kiếm - Mặc định là hôm nay
# Thay đổi thành số ngày trong quá khứ nếu muốn tìm email cũ hơn
# Ví dụ: 1 = hôm qua, 7 = một tuần trước
DAYS_TO_SEARCH = 0

# Mức log: WARNING = ít thông báo, INFO = nhiều thông báo hơn, DEBUG = rất chi tiết
LOG_LEVEL = logging.WARNING


# Thiết lập logging
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# =================================================================
# =================== Chương trình chính ==========================
# =================================================================

# ========== CẤU HÌNH LOGGING & EMAIL =============================
# =================================================================

# Email nhận báo cáo kết quả và log chạy script chi tiết
RESULT_RECEIVER_LIST = [
    "bac.dx@vietnamobile.com.vn",
    # "thanh.tv@vietnamobile.com.vn",
    # Thêm email người nhận khác vào đây
]

RESULT_EMAIL_SUBJECT = "[Automate Job Result]"

# =================================================================
# ========== LOGGER CLASS =========================================
# =================================================================

class Logger(object):
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()  # Ensure write to disk immediately

    def flush(self, *args, **kwargs):
        self.terminal.flush()
        self.log.flush()

def setup_logging(process_date):
    """Thiết lập logging vào file"""
    log_dir = "Log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    # Tên file log theo ngày xử lý (process_date)
    log_filename = f"log_{process_date.strftime('%Y-%m-%d')}.txt"
    log_path = os.path.join(log_dir, log_filename)
    
    # Redirect stdout và stderr vào file log
    sys.stdout = Logger(log_path)
    sys.stderr = sys.stdout
    
    return log_path


def _excel_cell_value(value):
    """Normalize Python/pandas/numpy values for openpyxl write_only cells."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return None
        return ts.to_pydatetime().replace(tzinfo=None)
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        f = float(value)
        return None if math.isnan(f) or math.isinf(f) else f
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def write_dataframe_to_excel_chunked(
    df: pd.DataFrame,
    file_path,
    chunk_size: int = 10000,
    sheet_name: str = "Sheet1",
):
    """Ghi DataFrame ra .xlsx theo chunk bằng openpyxl write_only (tiết kiệm RAM).

    ~180k dòng (30 ngày × ~6k) ghi dần thay vì build workbook full trong memory
    như pandas.DataFrame.to_excel().
    Ghi file tạm rồi replace atomic để tránh hỏng file gốc nếu bị gián đoạn.
    """
    file_path = Path(file_path)
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")

    total_rows = len(df)
    columns = list(df.columns)
    n_chunks = max(1, math.ceil(total_rows / chunk_size)) if total_rows else 1

    print(
        f"   💾 Chunked Excel write: {total_rows:,} rows, "
        f"chunk_size={chunk_size:,}, chunks={n_chunks}"
    )

    wb = Workbook(write_only=True)
    ws = wb.create_sheet(title=sheet_name)
    ws.append(columns)

    if total_rows > 0:
        # values ndarray + column-wise convert is faster than iterrows
        values = df.to_numpy(dtype=object, copy=False)
        for chunk_idx, start in enumerate(range(0, total_rows, chunk_size), start=1):
            end = min(start + chunk_size, total_rows)
            block = values[start:end]
            for row in block:
                ws.append([_excel_cell_value(v) for v in row])
            print(f"   … wrote rows {start + 1:,}-{end:,} / {total_rows:,} (chunk {chunk_idx}/{n_chunks})")

    try:
        wb.save(tmp_path)
        # Atomic replace on Windows if target exists
        os.replace(tmp_path, file_path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass

    print(f"   ✅ Chunked write done → {file_path}")


# =================================================================
# ========== MAIN SCRIPT ==========================================
# =================================================================

def main():
    # Thêm parser cho command-line arguments
    parser = argparse.ArgumentParser(description="Automated BI Report Generator.")
    parser.add_argument(
        "-s", "--skip-email",
        action="store_true",
        help="Skip email connection and download steps, process local files directly."
    )
    parser.add_argument(
        "-d", "--process-date",
        type=str,
        help="Specific date to process (YYYY-MM-DD). Defaults to yesterday if not provided."
    )
    args = parser.parse_args()

    # Determine processing date
    if args.process_date:
        try:
            process_date = datetime.strptime(args.process_date, "%Y-%m-%d")
        except ValueError:
            print("❌ Invalid date format. Please use YYYY-MM-DD.")
            return
    else:
        # Default to yesterday
        process_date = datetime.now() - timedelta(days=1)

    # Logic: Dữ liệu của ngày T (process_date) nằm trong email gửi ngày T+1
    # Do đó ngày tìm kiếm email phải là process_date + 1 ngày
    email_search_date = process_date + timedelta(days=1)
    
    # 1. Setup Logging
    log_path = setup_logging(process_date)
    print(f"📝 Log file: {log_path}")
    print(f"📅 User Selected Data Date: {process_date.strftime('%Y-%m-%d')}")
    print(f"📧 Email Search Date: {email_search_date.strftime('%Y-%m-%d')}")
    print(f"🕒 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.skip_email:
        print("\n" + "="*60)
        print("⏭️  --skip-email flag detected. Bỏ qua các bước download và giải nén.")
        print("   Đảm bảo file đã được giải nén và sẵn sàng trong thư mục 'downloads'.")
        print("="*60 + "\n")
    
    account = None
    current_step = "Initialization"
    
    try:
        if not args.skip_email:
            # 2. Kết nối Exchange
            current_step = "Connect Exchange"
            print("\n" + "="*60)
            print("🔌 KẾT NỐI EXCHANGE SERVER")
            print("="*60 + "\n")
            account = get_exchange_account()

            if not account:
                raise Exception("Không thể kết nối tới Exchange Server")

            # 3. Dọn dẹp thư mục downloads
            current_step = "Clean Downloads"
            print("\n" + "="*60)
            print("🧹 DỌN DẸP THƯ MỤC")
            print("="*60 + "\n")
            
            if os.path.exists(DOWNLOAD_FOLDER):
                shutil.rmtree(DOWNLOAD_FOLDER)
                print(f"✅ Đã xóa thư mục: {DOWNLOAD_FOLDER}")
            
            os.makedirs(DOWNLOAD_FOLDER)
            print(f"✅ Đã tạo lại thư mục: {DOWNLOAD_FOLDER}")

            # 4. Tìm và download từ danh sách subject của mình
            current_step = "Download Ericsson KPIs (Personal)"
            print("\n" + "="*60)
            print("📥 TẢI FILE TỪ EMAIL (Ericsson)")
            print("="*60 + "\n")
            
            results = find_and_download_emails(
                account=account,
                folder_name=FOLDER_NAME,
                sender_email=SENDER_EMAIL,
                subject_list=LIST_OF_EMAILS,
                download_folder=DOWNLOAD_FOLDER,
                days_back=DAYS_TO_SEARCH,
                allowed_extensions=ALLOWED_EXTENSIONS,
                target_date=email_search_date  # Pass email_search_date
            )

            # 5. Hiển thị kết quả chi tiết (tùy chọn)
            if results:
                print("\n📋 Chi tiết kết quả:")
                for subject, files in results.items():
                    if files:
                        print(f"  ✅ {subject}: {len(files)} file")
                    else:
                        print(f"  ❌ {subject}: Không tìm thấy file")

            # 6. Tìm và download từ danh sách subject của Z
            current_step = "Download ZTE KPIs (Shared)"
            print("\n" + "="*60)
            print("📥 TẢI FILE TỪ EMAIL (ZTE)")
            print("="*60 + "\n")
            
            results_z = find_and_download_emails(
                account=account,
                folder_name=FOLDER_NAME_Z,
                sender_email=SENDER_EMAIL_Z,
                subject_list=LIST_OF_EMAILS_Z,
                download_folder=DOWNLOAD_FOLDER,
                days_back=DAYS_TO_SEARCH,
                allowed_extensions=ALLOWED_EXTENSIONS,
                target_date=email_search_date  # Pass email_search_date
            )

            # 7. Hiển thị kết quả chi tiết (tùy chọn) của Z
            if results_z:
                print("\n📋 Chi tiết kết quả:")
                for subject, files in results_z.items():
                    if files:
                        print(f"  ✅ {subject}: {len(files)} file")
                    else:
                        print(f"  ❌ {subject}: Không tìm thấy file")

        # 8. Giải nén tất cả file ZIP trong thư mục downloads
        current_step = "Extract ZIPs"
        print("\n" + "="*60)
        print("📦 GIẢI NÉN FILE ZIP")
        print("="*60 + "\n")
        extract_all_zips(DOWNLOAD_FOLDER)

        # 9. Xử lý dữ liệu từ 4 processors
        current_step = "Data Processing"
        print("\n" + "="*60)
        print("🔄 XỬ LÝ DỮ LIỆU")
        print("="*60 + "\n")
        
        # Process 3G Ericsson
        current_step = "Processing 3G Ericsson"
        print("📊 Processing 3G Ericsson...")
        processor_3g_eric = Ericsson3GProcessor(download_folder=DOWNLOAD_FOLDER)
        processor_3g_eric.load_all_3g_data()
        processor_3g_eric.transform_all()
        processor_3g_eric.merge_final_result()
        processor_3g_eric.standardize_columns()
        processor_3g_eric.clean_data()
        df_3g_eric_site = processor_3g_eric.aggregate_by_site()
        print(f"✅ 3G Ericsson: {len(df_3g_eric_site):,} sites\n")
        
        # Process 3G ZTE
        current_step = "Processing 3G ZTE"
        print("📊 Processing 3G ZTE...")
        processor_3g_zte = ZTE3GProcessor(download_folder=DOWNLOAD_FOLDER)
        processor_3g_zte.load_all_3g_zte_data()
        processor_3g_zte.merge_final_result()
        processor_3g_zte.standardize_columns()
        processor_3g_zte.clean_data()
        df_3g_zte_site = processor_3g_zte.aggregate_by_site()
        print(f"✅ 3G ZTE: {len(df_3g_zte_site):,} sites\n")
        
        # Process 4G Ericsson
        current_step = "Processing 4G Ericsson"
        print("📊 Processing 4G Ericsson...")
        processor_4g_eric = Ericsson4GProcessor(download_folder=DOWNLOAD_FOLDER)
        processor_4g_eric.load_all_4g_ericsson_data()
        processor_4g_eric.merge_final_result()
        processor_4g_eric.standardize_columns()
        processor_4g_eric.clean_data()
        df_4g_eric_site = processor_4g_eric.aggregate_by_site()
        print(f"✅ 4G Ericsson: {len(df_4g_eric_site):,} sites\n")
        
        # Process 4G ZTE
        current_step = "Processing 4G ZTE"
        print("📊 Processing 4G ZTE...")
        processor_4g_zte = ZTE4GProcessor(download_folder=DOWNLOAD_FOLDER)
        processor_4g_zte.load_all_4g_zte_data()
        processor_4g_zte.merge_final_result()
        processor_4g_zte.standardize_columns()
        processor_4g_zte.clean_data()
        df_4g_zte_site = processor_4g_zte.aggregate_by_site()
        print(f"✅ 4G ZTE: {len(df_4g_zte_site):,} sites\n")
        
        # 10. Concat 3G data (Ericsson + ZTE)
        # Finished processing data for all files, now combine all data to final files
        print("\n" + "="*60)
        print("🔗 CONCATENATING DATA 3G AND 4G")
        print("="*60 + "\n")
        current_step = "Concatenating 3G Data"
        print("🔗 Concatenating 3G data...")
        df_3g_site = pd.concat([df_3g_eric_site, df_3g_zte_site], ignore_index=True)
        print(f"✅ 3G Combined: {len(df_3g_site):,} sites\n")
        
        # 11. Concat 4G data (Ericsson + ZTE)
        current_step = "Concatenating 4G Data"
        print("🔗 Concatenating 4G data...")
        df_4g_site = pd.concat([df_4g_eric_site, df_4g_zte_site], ignore_index=True)
        print(f"✅ 4G Combined: {len(df_4g_site):,} sites\n")
        
        print("\n" + "="*60)
        print("🔗 MERGING DATA 3G AND 4G TO ONE DATAFRAME")
        print("="*60 + "\n")
        # 12. Merge 3G + 4G data
        current_step = "Merging 3G + 4G Data"
        print("🔗 Merging 3G + 4G data...")
        df_site_data = pd.merge(
            df_3g_site,
            df_4g_site,
            on='SiteID',
            how='outer'
        ).fillna(0)
        print(f"✅ Site Data: {len(df_site_data):,} sites\n")
        
        # 13. Load SiteLocation and add location data
        # Skip this step - Long Lat will be added in another place

        # current_step = "Adding Location Data"
        # print("📍 Adding location data...")
        # site_location_path = Path(__file__).parent / "SiteLocation.csv"
        # df_location = pd.read_csv(site_location_path, usecols=['Site_ID', 'Long', 'Lat'])
        
        # Add Date column (based on process_date)
        df_site_data['Date'] = pd.to_datetime(process_date.strftime('%Y-%m-%d'))
        
        # Lookup Long and Lat
        # location_dict_long = dict(zip(df_location['Site_ID'], df_location['Long']))
        # location_dict_lat = dict(zip(df_location['Site_ID'], df_location['Lat']))
        
        # df_site_data['Long'] = df_site_data['SiteID'].map(location_dict_long).fillna(0)
        # df_site_data['Lat'] = df_site_data['SiteID'].map(location_dict_lat).fillna(0)
        
        # Reorder columns
        arranged_columns = [
            'Date', 'SiteID',
            '3G_User', '3G_Speed', '3G_Voice', '3G_Data',
            '4G_User', '4G_Speed', '4G_Voice', '4G_Data'
        ]
        df_site_data = df_site_data[arranged_columns]
        
        # Rename columns to final names
        final_column_names = {
            'SiteID': 'Site',
            '3G_User': '3G Sub',
            '3G_Speed': '3G Speed',
            '3G_Voice': '3G Voice traffic',
            '3G_Data': '3G Data traffic',
            '4G_User': '4G Sub',
            '4G_Speed': '4G Speed',
            '4G_Voice': '4G Voice traffic',
            '4G_Data': '4G Data traffic'
        }
        df_site_data = df_site_data.rename(columns=final_column_names)

        # print(f"✅ Added location data\n")
        
        # 13.5 Drop rows with Long = 0 or Lat = 0
        # current_step = "Filtering Invalid Coordinates"
        # print("🗺️ Filtering out sites with invalid coordinates...")
        # initial_count = len(df_site_data)
        # df_site_data = df_site_data[(df_site_data['Long'] != 0) & (df_site_data['Lat'] != 0)]
        # filtered_count = len(df_site_data)
        # dropped_count = initial_count - filtered_count
        # print(f"✅ Filtered out {dropped_count:,} sites with Long=0 or Lat=0")
        
        # 14. Save to Aggregate.xlsx
        current_step = "Saving to Excel"
        print("💾 Saving to Aggregate.xlsx...")
        aggregate_file = Path(__file__).parent.parent / "Aggregate.xlsx"
        print(f'Aggregate file path: {aggregate_file}')
        
        if aggregate_file.exists():
            # Load existing data
            df_existing = pd.read_excel(aggregate_file)
            
            # Append new data
            df_combined = pd.concat([df_existing, df_site_data], ignore_index=True)
            
            # Convert Date to datetime
            df_combined['Date'] = pd.to_datetime(df_combined['Date'])
            
            # Keep only last 30 days
            cutoff_date = datetime.now() - timedelta(days=30)
            df_combined = df_combined[df_combined['Date'] >= cutoff_date]
            
            # Sort by Date
            df_combined = df_combined.sort_values('Date')
            
            print(f"✅ Appended to existing file")
            print(f"📊 Total records: {len(df_combined):,} (last 30 days)")
        else:
            df_combined = df_site_data
            print(f"✅ Created new file")
            print(f"📊 Total records: {len(df_combined):,}")
        
        # Save to Excel (chunked write_only — ổn định hơn với ~180k dòng)
        write_dataframe_to_excel_chunked(
            df_combined,
            aggregate_file,
            chunk_size=10000,
        )
        print(f"✅ Saved to {aggregate_file.name}\n")
        
        # 14.5 Copy to external folder
        if PATH_TO_COPY:
            current_step = "Copying to external folder"
            try:
                print(f"📂 Copying to: {PATH_TO_COPY}")
                destination_dir = Path(PATH_TO_COPY)
                
                if not destination_dir.exists():
                    print(f"   Creating directory: {PATH_TO_COPY}")
                    destination_dir.mkdir(parents=True, exist_ok=True)
                
                destination_file = destination_dir / aggregate_file.name
                file_existed = destination_file.exists()
                
                shutil.copy2(aggregate_file, destination_file)
                
                if file_existed:
                    print(f"🔄 Đã ghi đè file tại: {destination_file}")
                else:
                    print(f"✅ Copy thành công file tới: {destination_file}")
            except Exception as e:
                print(f"⚠️ Failed to copy file: {e}")
                # Don't stop the process, just log warning
        
        print("="*60)
        print("🎉 DATA PROCESSING COMPLETED!")
        print("="*60)
        
        # 15. Gửi email thông báo thành công
        if account:
            notifier = Notifier(account, RESULT_RECEIVER_LIST, RESULT_EMAIL_SUBJECT)
            notifier.send_success(log_file=log_path)

    except Exception as e:
        print(f"\n❌ LỖI NGHIÊM TRỌNG TẠI BƯỚC: {current_step}")
        print(f"❌ Error details: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Gửi email thông báo lỗi
        if account:
            notifier = Notifier(account, RESULT_RECEIVER_LIST, RESULT_EMAIL_SUBJECT)
            notifier.send_failure(step_name=current_step, error_msg=str(e), log_file=log_path)
        else:
            try:
                # Chỉ thử kết nối lại nếu ban đầu chưa có account và không phải do skip_email
                if not args.skip_email:
                    account = get_exchange_account() # try to connect to exchange server again
                    if account:
                        notifier = Notifier(account, RESULT_RECEIVER_LIST, RESULT_EMAIL_SUBJECT)
                        notifier.send_failure(step_name=current_step, error_msg=str(e), log_file=log_path)
            except Exception as e:
                print(f"⚠️ Không thể gửi email báo lỗi, lý do: {str(e)}")

if __name__ == "__main__":
    main()