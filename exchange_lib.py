
import os
import logging
from dotenv import load_dotenv
from exchangelib import Credentials, Account, Configuration, DELEGATE, NTLM, EWSDateTime, FileAttachment, EWSTimeZone
from exchangelib.errors import AutoDiscoverFailed, TransportError, EWSWarning
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

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
    try:
        for child in parent_folder.children:
            if child.name.lower() == subfolder_name.lower():
                return child
    except Exception as e:
        log.warning(f"Lỗi khi tìm thư mục con: {e}")
    
    return None

def find_and_download_emails(account, folder_name, sender_email, subject_list, 
                           download_folder="downloads", days_back=0, 
                           allowed_extensions=None, target_date=None):
    """
    Tìm và download attachments từ danh sách email subjects
    
    Args:
        account: Đối tượng Account đã kết nối
        folder_name: Tên thư mục cần tìm
        sender_email: Email người gửi (None = không lọc)
        subject_list: Danh sách các tiêu đề email cần tìm
        download_folder: Thư mục lưu file tải về
        days_back: Số ngày tìm ngược về quá khứ (0 = hôm nay)
        allowed_extensions: Danh sách các định dạng được phép tải về
        target_date: Ngày cụ thể cần tìm (datetime object). Nếu có sẽ ưu tiên hơn days_back.
    
    Returns:
        Dictionary {subject: [file paths]} - kết quả download
    """
    if not subject_list:
        print("⚠️ Danh sách email trống. Vui lòng thêm ít nhất một tiêu đề email.")
        return {}
        
    # 1. Tìm thư mục
    folder = None
    if folder_name.lower() == 'inbox':
        folder = account.inbox
    else:
        # Tìm thư mục con trong inbox
        folder = find_subfolder(account.inbox, folder_name)
    
    if not folder:
        print(f"❌ Không tìm thấy thư mục '{folder_name}'")
        print("📂 Các thư mục con trong Inbox:")
        try:
            for child in account.inbox.children:
                print(f"   - {child.name}")
        except Exception:
            print("   Không thể liệt kê thư mục con")
        return {}
    
    print(f"🔍 Tìm kiếm trong thư mục: {folder.name}")
    
    # 2. Tạo khoảng thời gian tìm kiếm
    # Sử dụng múi giờ Việt Nam để đảm bảo tìm kiếm chính xác theo ngày địa phương
    try:
        tz = EWSTimeZone('Asia/Ho_Chi_Minh')
    except Exception:
        # Fallback nếu không set được
        tz = account.default_timezone
    
    if target_date:
        # Nếu có ngày cụ thể
        search_date = target_date.date() if isinstance(target_date, datetime) else target_date
    else:
        # Mặc định dùng days_back
        search_date = datetime.now().date() - timedelta(days=days_back)
    
    # Bắt đầu ngày
    start_day = EWSDateTime(
        year=search_date.year,
        month=search_date.month,
        day=search_date.day,
        hour=0, minute=0, second=0,
        tzinfo=tz,
    )
    
    # Kết thúc ngày
    end_day = EWSDateTime(
        year=search_date.year,
        month=search_date.month,
        day=search_date.day,
        hour=23, minute=59, second=59,
        tzinfo=tz,
    )
    
    print(f"📅 Ngày tìm kiếm: {search_date.strftime('%Y-%m-%d')}")
    
    # 3. Tạo thư mục download nếu chưa tồn tại
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
        print(f"📁 Đã tạo thư mục '{download_folder}'")
    
    # 4. Chuẩn bị kết quả
    result = {}
    
    # Chuẩn hóa các extension được phép
    if allowed_extensions:
        allowed_extensions = [ext.lower() if ext.startswith('.') else f".{ext.lower()}" 
                            for ext in allowed_extensions]
        print(f"🔍 Chỉ download các file có phần mở rộng: {', '.join(allowed_extensions)}")
    
    # 5. Lặp qua từng subject và tìm + download
    for subject in subject_list:
        print(f"\n🔎 Đang tìm email với tiêu đề: \"{subject}\"")
        
        # Query tìm email cho subject cụ thể
        query_params = {
            'datetime_received__gte': start_day,
            'datetime_received__lt': end_day,
            'has_attachments': True,
            'subject': subject  # Tìm chính xác subject
        }
        
        # Truy vấn tối ưu - chỉ lấy các trường cần thiết
        query = folder.filter(**query_params).order_by('-datetime_received')
        query = query.only(
            'subject', 'sender', 'datetime_received', 
            'has_attachments', 'attachments'
        )
        
        # Tìm email
        emails = []
        for item in query:
            # Kiểm tra người gửi nếu được chỉ định
            if sender_email and item.sender:
                if item.sender.email_address.lower() != sender_email.lower():
                    continue
            emails.append(item)
        
        if not emails:
            print(f"⚠️ Không tìm thấy email với tiêu đề \"{subject}\"")
            result[subject] = []
            continue
        
        print(f"📨 Tìm thấy {len(emails)} email phù hợp")
        
        # Download attachments
        downloaded_files = []
        
        for email_idx, item in enumerate(emails, start=1):
            if not item.attachments:
                continue
                
            print(f"  📧 Email #{email_idx}: {item.subject} - {item.datetime_received}")
            print(f"     Từ: {item.sender.email_address if item.sender else 'N/A'}")
                
            for att in item.attachments:
                if not isinstance(att, FileAttachment) or not hasattr(att, 'name'):
                    continue
                    
                filename = att.name
                _, ext = os.path.splitext(filename)
                
                # Kiểm tra extension nếu được chỉ định
                if allowed_extensions and ext.lower() not in allowed_extensions:
                    print(f"     ⏩ Bỏ qua {filename} (không thuộc định dạng cho phép)")
                    continue
                
                # Đường dẫn lưu file
                local_path = os.path.join(download_folder, filename)
                
                # Download file
                try:
                    with open(local_path, 'wb') as f:
                        f.write(att.content)
                    
                    size = os.path.getsize(local_path)
                    size_str = f"{size/1024:.1f} KB" if size < 1024*1024 else f"{size/(1024*1024):.1f} MB"
                    print(f"     ✅ Downloaded: {filename} ({size_str})")
                    downloaded_files.append(local_path)
                except Exception as e:
                    print(f"     ❌ Lỗi khi download {filename}: {e}")
        
        # Lưu kết quả cho subject này
        result[subject] = downloaded_files
        print(f"  📥 Đã tải {len(downloaded_files)} file cho \"{subject}\"")
    
    # 6. Hiển thị tổng kết
    total_files = sum(len(files) for files in result.values())
    print("\n📊 Tổng kết:")
    print(f"  🔍 Đã tìm {len(subject_list)} loại email")
    print(f"  📥 Tổng cộng tải về {total_files} file")
    print(f"  📁 Thư mục lưu file: {os.path.abspath(download_folder)}")
    
    return result

def send_email(account, recipients, subject, body, attachments=None):
    """
    Gửi email thông báo kết quả
    
    Args:
        account: Đối tượng Account đã kết nối
        recipients: List email người nhận
        subject: Tiêu đề email
        body: Nội dung email
        attachments: List đường dẫn file đính kèm (optional)
    """
    from exchangelib import Message, Mailbox, FileAttachment
    
    try:
        print(f"\n📧 Đang gửi email tới: {', '.join(recipients)}")
        
        # Tạo danh sách người nhận
        to_recipients = [Mailbox(email_address=r) for r in recipients]
        
        # Tạo message
        m = Message(
            account=account,
            folder=account.sent,
            subject=subject,
            body=body,
            to_recipients=to_recipients
        )
        
        # Đính kèm file nếu có
        if attachments:
            for filepath in attachments:
                if os.path.exists(filepath):
                    with open(filepath, 'rb') as f:
                        content = f.read()
                    
                    filename = os.path.basename(filepath)
                    file_att = FileAttachment(name=filename, content=content)
                    m.attach(file_att)
                    print(f"   📎 Đã đính kèm: {filename}")
                else:
                    print(f"   ⚠️ Không tìm thấy file đính kèm: {filepath}")
        
        # Gửi email
        m.send()
        print("✅ Gửi email thành công!")
        return True
        
    except Exception as e:
        print(f"❌ Lỗi khi gửi email: {str(e)}")
        return False
