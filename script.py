
import os
import logging
from dotenv import load_dotenv
from exchange_lib import get_exchange_account, find_and_download_emails
from extract_zippy import extract_all_zips
import shutil

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
DAYS_TO_SEARCH = 0

# Mức log: WARNING = ít thông báo, INFO = nhiều thông báo hơn, DEBUG = rất chi tiết
LOG_LEVEL = logging.WARNING


# Thiết lập logging
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# =================================================================
# =================== Chương trình chính ==========================
# =================================================================

def clear_download_folder(folder_path):
    """Xóa toàn bộ file trong thư mục"""
    if os.path.exists(folder_path):
        try:
            shutil.rmtree(folder_path)  # Xóa thư mục và toàn bộ nội dung
            os.makedirs(folder_path)     # Tạo lại thư mục trống
            print(f"🗑️ Đã xóa toàn bộ file trong thư mục '{folder_path}'")
        except Exception as e:
            print(f"❌ Lỗi khi xóa thư mục: {e}")
    else:
        # Tạo thư mục nếu chưa tồn tại
        os.makedirs(folder_path)
        print(f"📁 Đã tạo thư mục '{folder_path}'")


def main():
    """Quy trình chính"""
    # 1.1 Kết nối
    account = get_exchange_account()
    if not account:
        return

    # 1.2 Xóa thư mục download
    clear_download_folder(DOWNLOAD_FOLDER)

    # 2. Tìm và download từ danh sách subject
    results = find_and_download_emails(
        account=account,
        folder_name=FOLDER_NAME,
        sender_email=SENDER_EMAIL,
        subject_list=LIST_OF_EMAILS,
        download_folder=DOWNLOAD_FOLDER,
        days_back=DAYS_TO_SEARCH,
        allowed_extensions=ALLOWED_EXTENSIONS
    )

    # 3. Hiển thị kết quả chi tiết (tùy chọn)
    if results:
        print("\n📋 Chi tiết kết quả:")
        for subject, files in results.items():
            if files:
                print(f"  ✅ {subject}: {len(files)} file")
            else:
                print(f"  ❌ {subject}: Không tìm thấy file")

    # 4. Tìm và download từ danh sách subject của Z
    results_z = find_and_download_emails(
        account=account,
        folder_name=FOLDER_NAME_Z,
        sender_email=SENDER_EMAIL_Z,
        subject_list=LIST_OF_EMAILS_Z,
        download_folder=DOWNLOAD_FOLDER,
        days_back=DAYS_TO_SEARCH,
        allowed_extensions=ALLOWED_EXTENSIONS
    )

    # 5. Hiển thị kết quả chi tiết (tùy chọn) của Z
    if results_z:
        print("\n📋 Chi tiết kết quả:")
        for subject, files in results_z.items():
            if files:
                print(f"  ✅ {subject}: {len(files)} file")
            else:
                print(f"  ❌ {subject}: Không tìm thấy file")

    # 6. Giải nén tất cả file ZIP trong thư mục downloads
    print("\n" + "="*60)
    print("📦 GIẢI NÉN FILE ZIP")
    print("="*60 + "\n")
    extract_all_zips(DOWNLOAD_FOLDER)


# Chạy script

if __name__ == "__main__":
    main()
