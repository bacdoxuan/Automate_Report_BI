
# =============================================================================
# downloademailonly.py
#
# Download-only variant of the ETL pipeline: connects to Exchange, downloads
# email attachments, and extracts ZIP files. No data processing is performed.
# Supports --skip-email (process local files only), --process-date for
# specific date(s) (comma-separated list), e.g. "2026-10-01,2026-10-02".
# =============================================================================

import os
import sys
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

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

# Giữ tham chiếu tới stdout thật (Logger không nối chuỗi qua nhiều ngày)
_REAL_STDOUT = sys.stdout


class Logger(object):
    """Redirect stdout and stderr to both console and log file for full execution traceability."""
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
    """Set up logging to file named by processing date. Redirects stdout/stderr to log file.
    Args:
        process_date (datetime): The date being processed.
    Returns:
        str: Path to the created log file.
    """
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

# =================================================================
# ========== MAIN SCRIPT ==========================================
# =================================================================

def run_download_pipeline(process_date: datetime, args, shared_account=None) -> bool:
    """Download-only pipeline cho MỘT ngày xử lý.

    Steps: connect Exchange, clean downloads folder, download attachments
    (Ericsson personal + ZTE shared), extract ZIPs. No data processing.

    Args:
        process_date: Ngày dữ liệu cần xử lý.
        args: argparse Namespace.
        shared_account: Account Exchange đã kết nối sẵn (chế độ list ngày).
            Nếu None, tự kết nối (chế độ 1 ngày hoặc --skip-email).

    Returns:
        True nếu hoàn tất không lỗi, False nếu có lỗi (đã gửi notification,
        không raise).
    """
    from notification import Notifier
    from exchange_lib import get_exchange_account, find_and_download_emails
    from extract_zippy import extract_all_zips
    import shutil

    # Logic: Dữ liệu của ngày T (process_date) nằm trong email gửi ngày T+1
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
                target_date=email_search_date
            )

            if results:
                print("\n📋 Chi tiết kết quả:")
                for subject, files in results.items():
                    if files:
                        print(f"  ✅ {subject}: {len(files)} file")
                    else:
                        print(f"  ❌ {subject}: Không tìm thấy file")

            # 5. Tìm và download từ danh sách subject của Z
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
                target_date=email_search_date
            )

            if results_z:
                print("\n📋 Chi tiết kết quả:")
                for subject, files in results_z.items():
                    if files:
                        print(f"  ✅ {subject}: {len(files)} file")
                    else:
                        print(f"  ❌ {subject}: Không tìm thấy file")

        # 6. Giải nén tất cả file ZIP trong thư mục downloads
        current_step = "Extract ZIPs"
        print("\n" + "="*60)
        print("📦 GIẢI NÉN FILE ZIP")
        print("="*60 + "\n")
        extract_all_zips(DOWNLOAD_FOLDER)

        print("="*60)
        print("🎉 DOWNLOAD & EXTRACT COMPLETED!")
        print("="*60)

        # 7. Gửi email thông báo thành công
        if account:
            # H3: lỗi gửi mail không được biến một ngày đã tải xong thành FAIL
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

        notifier = None
        if account:
            notifier = Notifier(account, RESULT_RECEIVER_LIST, RESULT_EMAIL_SUBJECT)
        else:
            try:
                # Chỉ thử kết nối lại nếu ban đầu chưa có account và không phải do skip_email
                if not args.skip_email and shared_account is None:
                    from exchange_lib import get_exchange_account
                    account = get_exchange_account()
                    if account:
                        notifier = Notifier(account, RESULT_RECEIVER_LIST, RESULT_EMAIL_SUBJECT)
            except Exception as conn_err:
                print(f"⚠️ Không thể gửi email báo lỗi, lý do: {str(conn_err)}")

        if notifier:
            try:
                notifier.send_failure(step_name=current_step, error_msg=str(e), log_file=log_path)
            except Exception as mail_err:
                print(f"⚠️  Không gửi được email báo lỗi: {mail_err}")
        else:
            print("⚠️  Không có kết nối Exchange nên không gửi được email báo lỗi.")

        return False


def main() -> int:
    """Entry point: hỗ trợ backfill nhiều ngày (chỉ download + extract).

    - ``--process-date`` nhận MỘT ngày (YYYY-MM-DD) HOẶC LIST các ngày cách
      nhau bằng dấu ',' (ví dụ: 2026-10-01,2026-10-02,2026-10-03).
    - Mỗi ngày chạy một vòng pipeline riêng (log riêng), giữ nguyên thứ tự
      user nhập.
    - Nếu một ngày bị lỗi: BYPASS và tiếp tục ngày tiếp theo.
    - Kết thúc in tổng kết OK/FAILED.

    Returns:
        int: exit code. 0 = tất cả OK, 1 = có ngày lỗi, 2 = tham số ngày sai.
    """
    # Dùng chung parser + parse date với script.py (không import script.py
    # để tránh kéo theo pandas/numpy/4 processor).
    from cli_utils import build_arg_parser, parse_date_list

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
    print(f"🗓️  PLAN (download only): {len(date_list)} ngày cần xử lý")
    for i, d in enumerate(date_list, start=1):
        print(f"   {i}. {d.strftime('%Y-%m-%d')} "
              f"(email search date: {(d + timedelta(days=1)).strftime('%Y-%m-%d')})")
    print("=" * 60)

    # Kết nối Exchange LẦN DUY NHẤT trước loop (nếu chạy full mode)
    account = None
    if not args.skip_email:
        from exchange_lib import get_exchange_account
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

        # B1: khởi tạo TRƯỚC try (tránh UnboundLocalError / dùng lại giá trị ngày trước)
        success = False
        error_msg = None
        try:
            success = run_download_pipeline(process_date, args, shared_account=account)
            if not success:
                error_msg = "Error (see log)"
        except Exception as e:
            import traceback
            traceback.print_exc()
            error_msg = str(e)

        results.append((process_date.strftime("%Y-%m-%d"), success, error_msg))

        date_str = process_date.strftime("%Y-%m-%d")
        if success:
            print(f"✅ [{date_str}] Hoàn tất.")
        else:
            print(f"⚠️  [{date_str}] BỊ LỖI - Bypass, tiếp tục ngày tiếp theo...")

    # Tổng kết
    print("\n" + "=" * 60)
    print("📊 TỔNG KẾT BACKFILL (DOWNLOAD ONLY)")
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
