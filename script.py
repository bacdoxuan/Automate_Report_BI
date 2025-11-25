
import os
import logging
from dotenv import load_dotenv
from exchange_lib import get_exchange_account, find_and_download_emails

# Load biến môi trường từ .env
load_dotenv()

# =================================================================
# ========== CẤU HÌNH - CÓ THỂ THAY ĐỔI Ở ĐÂY =====================
# =================================================================

# Thư mục tìm kiếm (tên chính xác trong Exchange)
FOLDER_NAME = "Myself"

# Email người gửi (None = không lọc theo người gửi)
SENDER_EMAIL = "bac.dx@vietnamobile.com.vn"

# Danh sách tiêu đề email cần tìm
LIST_OF_EMAILS = [
    "Automate_3G_Throughput",
    "Automate_3G_Traffic_User",
    "Automate_VoLTE_Traffic_Ericsson",
    "Automate_North_LTE_Traffic_Data",
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

def main():
    """Quy trình chính"""
    # 1. Kết nối
    account = get_exchange_account()
    if not account:
        return
    
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
# Chạy script

if __name__ == "__main__":
    main()
