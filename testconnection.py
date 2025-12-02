import os
import logging
from dotenv import load_dotenv
from exchangelib import Credentials, Account, Configuration, DELEGATE, NTLM, Q
from exchangelib.errors import AutoDiscoverFailed, TransportError, EWSWarning

# Logging để debug khi lỗi
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Load biến môi trường từ .env
load_dotenv()

EMAIL_ADDRESS = os.getenv('EMAIL_ADDRESS')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
EXCHANGE_SERVER = os.getenv('EXCHANGE_SERVER')
EXCHANGE_DOMAIN = os.getenv('EXCHANGE_DOMAIN')
EXCHANGE_USERNAME = os.getenv('EXCHANGE_USERNAME')

def test_exchange_connection():
    """
    Kết nối đến Exchange server và trả về True/False,
    đồng thời in ra thông báo kết quả.
    """
    # 1. Kiểm tra đủ cấu hình chưa
    if not all([EMAIL_ADDRESS, EMAIL_PASSWORD, EXCHANGE_SERVER,
                EXCHANGE_DOMAIN, EXCHANGE_USERNAME]):
        print("❌ Thiếu cấu hình. Hãy kiểm tra lại file .env.")
        print(f"EMAIL_ADDRESS = {EMAIL_ADDRESS}")
        print(f"EXCHANGE_SERVER = {EXCHANGE_SERVER}")
        print(f"EXCHANGE_DOMAIN = {EXCHANGE_DOMAIN}")
        print(f"EXCHANGE_USERNAME = {EXCHANGE_USERNAME}")
        return False

    print(f"🔌 Đang thử kết nối đến Exchange server: {EXCHANGE_SERVER}")
    print(f"   Email   : {EMAIL_ADDRESS}")
    print(f"   Account : {EXCHANGE_DOMAIN}\\{EXCHANGE_USERNAME}")

    try:
        # 2. Tạo credentials: domain\username
        credentials = Credentials(
            username=f"{EXCHANGE_DOMAIN}\\{EXCHANGE_USERNAME}",
            password=EMAIL_PASSWORD,
        )

        # 3. Cấu hình kết nối
        config = Configuration(
            server=EXCHANGE_SERVER,   # có thể là hostname hoặc URL EWS
            credentials=credentials,
            auth_type=NTLM,           # thường dùng cho Exchange on-prem
        )

        # 4. Tạo Account (không dùng autodiscover vì đã chỉ rõ server)
        account = Account(
            primary_smtp_address=EMAIL_ADDRESS,
            config=config,
            autodiscover=False,
            access_type=DELEGATE,
        )

        # 5. Gọi thử 1 request đơn giản để ép nó connect thật
        inbox_name = account.inbox.name
        print(f"📂 Truy cập được folder: {inbox_name}")

        print(f"✅ Kết nối Exchange thành công cho: {EMAIL_ADDRESS}")
        return True

    except AutoDiscoverFailed as e:
        print("❌ AutoDiscoverFailed (dù ta đang để autodiscover=False, "
              "có thể vẫn phát sinh nếu exchangelib cố fallback).")
        print(f"Chi tiết: {e}")
        log.exception("AutoDiscoverFailed")
    except TransportError as e:
        print("❌ TransportError: Không kết nối được tới server.")
        print("   → Kiểm tra lại:")
        print("     - Địa chỉ EXCHANGE_SERVER đúng chưa (hostname / URL EWS)")
        print("     - Có đi qua proxy / firewall / VPN không")
        print("     - Server có ping / curl được không")
        print(f"Chi tiết: {e}")
        log.exception("TransportError")
    except EWSWarning as e:
        print("⚠️ EWSWarning: Có cảnh báo từ server nhưng không hẳn là fail.")
        print(f"Chi tiết: {e}")
        log.exception("EWSWarning")
        # Tùy bạn quyết: coi là success hay fail
        return True
    except Exception as e:
        print("❌ Lỗi không xác định khi kết nối Exchange:")
        print(f"Chi tiết: {e}")
        log.exception("Unhandled exception")

    return False

if __name__ == "__main__":
    test_exchange_connection()