import os
import logging
from dotenv import load_dotenv
from exchangelib import Credentials, Account, Configuration, DELEGATE, NTLM, EWSDateTime, FileAttachment
from exchangelib.errors import AutoDiscoverFailed, TransportError, EWSWarning
from datetime import datetime
from testconnection import test_exchange_connection
from exchangelib import EWSDateTime

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

# account
def get_exchange_account():
    """Kết nối Exchange và trả về account object"""
    load_dotenv()
    
    try:
        credentials = Credentials(
            username=f"{os.getenv('EXCHANGE_DOMAIN')}\\{os.getenv('EXCHANGE_USERNAME')}",
            password=os.getenv('EMAIL_PASSWORD'),
        )
        config = Configuration(
            server=os.getenv('EXCHANGE_SERVER'),
            credentials=credentials,
            auth_type=NTLM,
        )
        account = Account(
            primary_smtp_address=os.getenv('EMAIL_ADDRESS'),
            config=config,
            autodiscover=False,
            access_type=DELEGATE,
        )
        print(f"✅ Kết nối Exchange thành công cho: {os.getenv('EMAIL_ADDRESS')}")
        return account
    except Exception as e:
        print(f"❌ Lỗi khi kết nối: {e}")
        return None


def find_subfolder(parent_folder, subfolder_name):
    """Tìm thư mục con theo tên"""
    for child in parent_folder.children:
        if child.name.lower() == subfolder_name.lower():
            return child
    return None


def find_today_emails_optimized(account, folder_name, sender_email, subject_exact):
    """
    Tìm email một cách hiệu quả
    - Chỉ tìm trong thư mục đã biết
    - Filter trực tiếp subject, không lọc sau
    - Dùng only() để giảm lượng dữ liệu tải về
    """
    # 1. Tìm thư mục (inbox hoặc subfolder)
    folder = None
    if folder_name.lower() == 'inbox':
        folder = account.inbox
    else:
        # Tìm thư mục con trong inbox
        folder = find_subfolder(account.inbox, folder_name)
    
    if not folder:
        print(f"❌ Không tìm thấy thư mục '{folder_name}'")
        return []
    
    print(f"🔍 Tìm kiếm trong thư mục: {folder.name}")
    
    # 2. Tạo khoảng thời gian ngày hôm nay 
    tz = account.default_timezone
    today = datetime.now().date()
    
    start = EWSDateTime(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=0, minute=0, second=0,
        tzinfo=tz,
    )
    
    end = EWSDateTime(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=23, minute=59, second=59,
        tzinfo=tz,
    )
    
    print(f"📅 Ngày tìm kiếm: {today.strftime('%Y-%m-%d')}")
    
    # 3. Xây dựng query - bao gồm cả subject & sender để lọc server-side
    query_params = {
        'datetime_received__gte': start,
        'datetime_received__lt': end,
        'has_attachments': True,
    }
    
    # Thêm subject nếu được chỉ định
    if subject_exact:
        query_params['subject'] = subject_exact
    
    # 4. Truy vấn tối ưu - only() để chỉ lấy các trường cần thiết
    # Giảm đáng kể lượng dữ liệu phải truyền qua mạng
    query = folder.filter(**query_params).order_by('-datetime_received')
    
    # Chỉ lấy các trường cần thiết
    query = query.only(
        'subject', 'sender', 'datetime_received', 
        'has_attachments', 'attachments', 'to_recipients'
    )
    
    # 5. Thực hiện truy vấn và filter theo sender (nếu cần)
    emails = []
    for item in query:
        if sender_email and item.sender:
            if item.sender.email_address.lower() != sender_email.lower():
                continue
        emails.append(item)
    
    # 6. Hiển thị kết quả
    print(f"📨 Tìm thấy {len(emails)} email phù hợp")
    
    for idx, item in enumerate(emails, start=1):
        print(f"\n===== EMAIL #{idx} =====")
        print(f"From    : {item.sender.email_address if item.sender else 'N/A'}")
        print(f"Subject : {item.subject}")
        print(f"Received: {item.datetime_received}")
        
        # Chỉ hiển thị tên file, không load nội dung
        if item.attachments:
            print("📎 Attachments:")
            for att_idx, att in enumerate(item.attachments, start=1):
                if isinstance(att, FileAttachment):
                    print(f"   {att_idx}. {att.name}")
                else:
                    print(f"   {att_idx}. (attachment type: {type(att).__name__})")
    
    return emails
def download_attachments_optimized(items, download_folder="downloads"):
    """Download attachments - phiên bản tối ưu hơn"""
    if not items:
        print("❌ Không có email nào để download")
        return []
    
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
        print(f"📁 Đã tạo thư mục '{download_folder}'")
    
    downloaded_files = []
    
    for email_idx, item in enumerate(items, start=1):
        if not item.attachments:
            continue
            
        for att in item.attachments:
            if not isinstance(att, FileAttachment):
                continue
                
            filename = att.name
            local_path = os.path.join(download_folder, filename)
            
            # Xử lý trùng tên
            if os.path.exists(local_path):
                name, ext = os.path.splitext(filename)
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                filename = f"{name}_{timestamp}{ext}"
                local_path = os.path.join(download_folder, filename)
            
            # Download
            with open(local_path, 'wb') as f:
                f.write(att.content)
            
            size = os.path.getsize(local_path)
            size_str = f"{size/1024:.1f} KB" if size < 1024*1024 else f"{size/(1024*1024):.1f} MB"
            
            print(f"✅ Downloaded: {filename} ({size_str})")
            downloaded_files.append(local_path)
    
    print(f"\n📊 Tổng cộng: {len(downloaded_files)} file đã tải về '{download_folder}'")
    return downloaded_files

def main_optimized():
    """Quy trình tối ưu hoàn chỉnh"""
    # 1. Kết nối
    account = get_exchange_account()
    if not account:
        return
    
    # 2. Tìm email trực tiếp - không cần liệt kê thư mục
    emails = find_today_emails_optimized(
        account=account,
        folder_name="Myself",  # Thư mục đã biết 
        sender_email="bac.dx@vietnamobile.com.vn",
        subject_exact="Automate_3G_Throughput"
    )
    
    # 3. Download nếu tìm thấy
    if emails:
        download_attachments_optimized(
            items=emails,
            download_folder="downloads"
        )
    else:
        print("❌ Không tìm thấy email phù hợp")
# Chạy quy trình tối ưu
if __name__ == "__main__":
    main_optimized()