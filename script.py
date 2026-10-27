
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
from cli_utils import build_arg_parser, parse_date_list  # noqa: F401  (re-export cho app_gradio)

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

# Số ngày lưu trong Aggregate.xlsx (cửa sổ retention). Dòng cũ hơn sẽ bị lọc bỏ.
RETENTION_DAYS = 30

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

# Giữ tham chiếu tới stdout thật để Logger không nối chuỗi qua nhiều ngày
# (khi backfill nhiều ngày, mỗi ngày tạo một Logger mới nhưng luôn ghi ra console gốc).
_REAL_STDOUT = sys.stdout


class Logger(object):
    def __init__(self, filename):
        self.terminal = _REAL_STDOUT
        self.log = open(filename, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()  # Ensure write to disk immediately

    def flush(self, *args, **kwargs):
        self.terminal.flush()
        self.log.flush()

    def isatty(self):
        return False

    def close(self):
        """Đóng file log (tránh rò file handle khi chạy nhiều ngày)."""
        try:
            self.log.close()
        except Exception:
            pass


def setup_logging(process_date):
    """Thiết lập logging vào file"""
    log_dir = "Log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    # Tên file log theo ngày xử lý (process_date)
    log_filename = f"log_{process_date.strftime('%Y-%m-%d')}.txt"
    log_path = os.path.join(log_dir, log_filename)

    # Đóng Logger của ngày trước (nếu có) để log không bị nối chéo giữa các ngày
    if isinstance(sys.stdout, Logger):
        sys.stdout.close()

    # Redirect stdout và stderr vào file log
    sys.stdout = Logger(log_path)
    sys.stderr = sys.stdout

    return log_path


def retention_cutoff():
    """Ngày sớm nhất còn được giữ trong Aggregate.xlsx (đã normalize về 00:00).

    Dùng so sánh theo NGÀY (không theo giờ) để cửa sổ retention đúng
    RETENTION_DAYS ngày, tránh mất thêm 1 ngày do lệch giờ.
    """
    return (datetime.now() - timedelta(days=RETENTION_DAYS)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


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


def merge_into_aggregate(df_site_data: pd.DataFrame, aggregate_file: Path, process_date) -> pd.DataFrame:
    """Ghi dữ liệu của MỘT ngày vào Aggregate.xlsx (idempotent, có retention).

    - H2: xoá dòng cũ của đúng ``process_date`` trước khi append, nên chạy lại
      cùng một ngày không sinh bản ghi trùng và backfill lại ngày lỗi là an toàn.
    - H1: retention so sánh theo NGÀY (normalize), không theo giờ.

    Returns:
        DataFrame đã ghi ra file.
    """
    print("💾 Saving to Aggregate.xlsx...")

    if aggregate_file.exists():
        # Load existing data
        df_existing = pd.read_excel(aggregate_file)

        # H2: xoá dữ liệu cũ của ĐÚNG ngày này trước khi append.
        target_day = pd.Timestamp(process_date).normalize()
        df_existing['Date'] = pd.to_datetime(df_existing['Date']).dt.normalize()
        removed = int((df_existing['Date'] == target_day).sum())
        if removed:
            df_existing = df_existing[df_existing['Date'] != target_day]
            print(f"   ♻️  Đã xoá {removed:,} dòng cũ của ngày {target_day:%Y-%m-%d} (trước khi ghi lại)")

        df_combined = pd.concat([df_existing, df_site_data], ignore_index=True)
        df_combined['Date'] = pd.to_datetime(df_combined['Date'])

        # H1: giữ đúng RETENTION_DAYS ngày (so sánh theo ngày).
        cutoff_date = retention_cutoff()
        before_rows = len(df_combined)
        df_combined = df_combined[df_combined['Date'].dt.normalize() >= cutoff_date]
        dropped_rows = before_rows - len(df_combined)
        if dropped_rows:
            print(f"   🗑️  Đã bỏ {dropped_rows:,} dòng ngoài cửa sổ "
                  f"{RETENTION_DAYS} ngày (cutoff {cutoff_date:%Y-%m-%d})")

        df_combined = df_combined.sort_values('Date')
        print("✅ Appended to existing file")
        print(f"📊 Total records: {len(df_combined):,} (last {RETENTION_DAYS} days)")
    else:
        df_combined = df_site_data
        print("✅ Created new file")
        print(f"📊 Total records: {len(df_combined):,}")

    write_dataframe_to_excel_chunked(df_combined, aggregate_file, chunk_size=10000)
    print(f"✅ Saved to {aggregate_file.name}\n")
    return df_combined


def run_pipeline(process_date: datetime, args, shared_account=None) -> bool:
    """Chạy đầy đủ pipeline ETL cho MỘT ngày xử lý.

    Steps: connect Exchange, clean downloads folder, download attachments
    (Ericsson personal + ZTE shared), extract ZIPs, process data (4G/3G
    Ericsson/ZTE), save Aggregate.xlsx, copy backup, send notification.

    Args:
        process_date: Ngày dữ liệu cần xử lý (YYYY-MM-DD).
        args: argparse Namespace (đưa vào để tránh parse lại sys.argv).
        shared_account: Account Exchange đã kết nối sẵn (chế độ chạy theo
            list nhiều ngày). Nếu None, pipeline tự kết nối (chế độ 1 ngày
            hoặc --skip-email).

    Returns:
        True nếu pipeline hoàn tất không lỗi, False nếu có lỗi (đã xử lý
        notification bên trong, không raise).
    """
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

    account = shared_account
    current_step = "Initialization"

    try:
        if not args.skip_email:
            # 2. Kết nối Exchange (dùng kết nối đã có nếu chạy theo list ngày)
            current_step = "Connect Exchange"
            print("\n" + "="*60)
            print("🔌 KẾT NỐI EXCHANGE SERVER")
            print("="*60 + "\n")
            if account is not None:
                # Đã kết nối ở main() cho cả list - không kết nối lại
                print("ℹ️  Sử dụng kết nối Exchange đã thiết lập từ trước (chế độ list ngày).")
            else:
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
        aggregate_file = Path(__file__).parent.parent / "Aggregate.xlsx"
        print(f"Aggregate file path: {aggregate_file}")
        merge_into_aggregate(df_site_data, aggregate_file, process_date)

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
        # H3: bọc riêng để lỗi gửi mail không biến một ngày đã ghi Excel thành FAIL.
        if account:
            try:
                notifier = Notifier(account, RESULT_RECEIVER_LIST, RESULT_EMAIL_SUBJECT)
                notifier.send_success(log_file=log_path)
            except Exception as mail_err:
                print(f"⚠️  Không gửi được email thông báo thành công: {mail_err}")

        return True

    except Exception as e:
        print(f"\n❌ LỖI NGHIÊM TRỌNG TẠI BƯỚC: {current_step}")
        print(f"❌ Error details: {str(e)}")
        import traceback
        traceback.print_exc()

        # Gửi email thông báo lỗi
        if account:
            try:
                notifier = Notifier(account, RESULT_RECEIVER_LIST, RESULT_EMAIL_SUBJECT)
                notifier.send_failure(step_name=current_step, error_msg=str(e), log_file=log_path)
            except Exception as mail_err:
                print(f"⚠️  Không gửi được email báo lỗi: {mail_err}")
        else:
            try:
                # Chỉ thử kết nối lại nếu ban đầu chưa có account và không phải do skip_email
                if not args.skip_email and shared_account is None:
                    account = get_exchange_account() # try to connect to exchange server again
                    if account:
                        notifier = Notifier(account, RESULT_RECEIVER_LIST, RESULT_EMAIL_SUBJECT)
                        notifier.send_failure(step_name=current_step, error_msg=str(e), log_file=log_path)
            except Exception as conn_err:
                print(f"⚠️  Không thể gửi email báo lỗi, lý do: {str(conn_err)}")

        return False


def main() -> int:
    """Entry point: hỗ trợ backfill nhiều ngày.

    - ``--process-date`` nhận MỘT ngày (YYYY-MM-DD) HOẶC LIST các ngày
      cách nhau bằng dấu ',' (ví dụ: 2026-10-01,2026-10-02,2026-10-03).
    - Mỗi ngày chạy một vòng pipeline riêng (log riêng, append riêng vào
      Aggregate.xlsx), giữ nguyên thứ tự user nhập.
    - Nếu một ngày bị lỗi dữ liệu: BYPASS (in cảnh báo + email báo lỗi)
      và tiếp tục chạy ngày tiếp theo.
    - Kết thúc in tổng kết OK/FAILED.

    Returns:
        int: exit code cho scheduler/subprocess.
        0 = tất cả ngày thành công, 1 = có ngày lỗi/không chạy được,
        2 = tham số ngày không hợp lệ.
    """
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.process_date:
        try:
            date_list = parse_date_list(args.process_date)
        except ValueError as e:
            print(f"❌ {e}")
            return 2
        if not date_list:
            print("❌ Không có ngày hợp lệ nào được chỉ định. "
                  "Vui lòng dùng định dạng YYYY-MM-DD (nhiều ngày cách nhau bằng dấu ',').")
            return 2
    else:
        date_list = [datetime.now() - timedelta(days=1)]

    print("=" * 60)
    print(f"🗓️  PLAN: {len(date_list)} ngày cần xử lý")
    for i, d in enumerate(date_list, start=1):
        print(f"   {i}. {d.strftime('%Y-%m-%d')} "
              f"(email search date: {(d + timedelta(days=1)).strftime('%Y-%m-%d')})")
    print("=" * 60)

    # H1: cảnh báo SỚM nếu ngày nằm ngoài cửa sổ retention
    # (dữ liệu vẫn được xử lý nhưng sẽ bị lọc khi ghi Aggregate.xlsx)
    cutoff = retention_cutoff()
    too_old = [d for d in date_list if d.date() < cutoff.date()]
    if too_old:
        print(f"\n⚠️  CẢNH BÁO: {len(too_old)} ngày nằm NGOÀI cửa sổ lưu trữ "
              f"{RETENTION_DAYS} ngày (cutoff {cutoff:%Y-%m-%d}).")
        for d in too_old:
            print(f"   - {d.strftime('%Y-%m-%d')} sẽ bị lọc bỏ khi ghi Aggregate.xlsx")
        print("   → Tăng RETENTION_DAYS trong script.py nếu cần backfill xa hơn.\n")

    # Kết nối Exchange LẦN DUY NHẤT trước loop (nếu chạy full mode)
    account = None
    if not args.skip_email:
        print("\n" + "=" * 60)
        print("🔌 KẾT NỐI EXCHANGE SERVER (chung cho cả danh sách ngày)")
        print("=" * 60 + "\n")
        account = get_exchange_account()
        if not account:
            print("❌ Không thể kết nối tới Exchange Server - KHÔNG THỂ CHẠY danh sách ngày.")
            return 1

    results = []  # list of (date_str, success_bool, error_msg_or_None)

    for idx, process_date in enumerate(date_list, start=1):
        print("\n" + "#" * 60)
        print(f"### [{idx}/{len(date_list)}] PROCESS DATE: {process_date.strftime('%Y-%m-%d')}")
        print("#" * 60 + "\n")

        # B1: khởi tạo TRƯỚC try để không dùng lại giá trị của ngày trước
        # và không UnboundLocalError khi ngày đầu tiên lỗi.
        success = False
        error_msg = None
        try:
            success = run_pipeline(
                process_date,
                args,
                shared_account=account,
            )
            if not success:
                error_msg = "Error (see log)"
        except Exception as e:
            # Bất kỳ lỗi nào phát sinh từ run_pipeline (thường không có vì
            # run_pipeline đã catch) cũng được bypass và tiếp tục ngày kế tiếp.
            import traceback
            traceback.print_exc()
            error_msg = str(e)

        results.append((process_date.strftime("%Y-%m-%d"), success, error_msg))

        # Hiển thị trạng thái tức thì
        date_str = process_date.strftime("%Y-%m-%d")
        if success:
            print(f"✅ [{date_str}] Hoàn tất.")
        else:
            print(f"⚠️  [{date_str}] BỊ LỖI - Bypass, tiếp tục ngày tiếp theo...")

    # Tổng kết
    print("\n" + "=" * 60)
    print("📊 TỔNG KẾT BACKFILL")
    print("=" * 60)
    for date_str, success, err in results:
        mark = "✅ OK" if success else "❌ FAIL"
        extra = f" - {err}" if err else ""
        print(f"  {mark}  {date_str}{extra}")
    ok_count = sum(1 for _, s, _ in results if s)
    print(f"\n  Tổng: {ok_count}/{len(results)} ngày thành công.")
    if ok_count < len(results):
        print("  ⚠️  Có ngày bị lỗi đã được bypass. Xem log chi tiết trong thư mục Log/.")

    # H4: exit code để scheduler/subprocess phân biệt OK/NOK
    return 0 if ok_count == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
