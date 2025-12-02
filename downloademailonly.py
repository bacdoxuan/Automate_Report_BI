
import os
import sys
import logging
import pandas as pd
from notification import Notifier
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from exchange_lib import get_exchange_account, find_and_download_emails
from extract_zippy import extract_all_zips
from processing_3G_Ericsson import Ericsson3GProcessor
from processing_3G_ZTE import ZTE3GProcessor
from processing_4G_Ericsson import Ericsson4GProcessor
from processing_4G_ZTE import ZTE4GProcessor
import shutil
import argparse

# Load biến môi trường từ .env
load_dotenv()

# =================================================================
# ========== CẤU HÌNH - CÓ THỂ THAY ĐỔI Ở ĐÂY =====================
# =================================================================

# Thư mục tìm kiếm (tên chính xác trong Exchange)
FOLDER_NAME = "Myself"
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

# Chỉ download các định dạng file này (để trống = tất cả file)
# Ví dụ: [".xlsx", ".pdf", ".csv"]
ALLOWED_EXTENSIONS = []

# Thời gian tìm kiếm - Mặc định là hôm nay
# Thay đổi thành số ngày trong quá khứ nếu muốn tìm email cũ hơn
# Ví dụ: 1 = hôm qua, 7 = một tuần trước
DAYS_TO_SEARCH = 1

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

# Email nhận báo cáo kết quả
RESULT_RECEIVER_LIST = [
    "bac.dx@vietnamobile.com.vn",
    "thanh.tv@vietnamobile.com.vn",
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

def setup_logging():
    """Thiết lập logging vào file"""
    log_dir = "Log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    # Tên file log theo ngày hôm qua (ngày dữ liệu) hoặc hôm nay (ngày chạy)
    # User yêu cầu log_<<yesterday>>.txt
    yesterday = datetime.now() - timedelta(days=1)
    log_filename = f"log_{yesterday.strftime('%Y-%m-%d')}.txt"
    log_path = os.path.join(log_dir, log_filename)
    
    # Redirect stdout và stderr vào file log
    sys.stdout = Logger(log_path)
    sys.stderr = sys.stdout
    
    return log_path

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
    args = parser.parse_args()

    # 1. Setup Logging
    log_path = setup_logging()
    print(f"📝 Log file: {log_path}")
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
            current_step = "Download Pass 1 (Personal)"
            print("\n" + "="*60)
            print("📥 TẢI FILE TỪ EMAIL (PASS 1)")
            print("="*60 + "\n")
            
            results = find_and_download_emails(
                account=account,
                folder_name=FOLDER_NAME,
                sender_email=SENDER_EMAIL,
                subject_list=LIST_OF_EMAILS,
                download_folder=DOWNLOAD_FOLDER,
                days_back=DAYS_TO_SEARCH,
                allowed_extensions=ALLOWED_EXTENSIONS
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
            current_step = "Download Pass 2 (Shared)"
            print("\n" + "="*60)
            print("📥 TẢI FILE TỪ EMAIL (PASS 2)")
            print("="*60 + "\n")
            
            results_z = find_and_download_emails(
                account=account,
                folder_name=FOLDER_NAME_Z,
                sender_email=SENDER_EMAIL_Z,
                subject_list=LIST_OF_EMAILS_Z,
                download_folder=DOWNLOAD_FOLDER,
                days_back=DAYS_TO_SEARCH,
                allowed_extensions=ALLOWED_EXTENSIONS
            )

            # 7. Hiển thị kết quả chi tiết (tùy chọn) của Z
            if results_z:
                print("\n📋 Chi tiết kết quả:")
                for subject, files in results_z.items():
                    if files:
                        print(f"  ✅ {subject}: {len(files)} file")
                    else:
                        print(f"  ❌ {subject}: Không tìm thấy file")
    except Exception as e:
        print(f"❌ Lỗi trong quá trình tải file: {str(e)}")
        raise

if __name__ == "__main__":
    main()
