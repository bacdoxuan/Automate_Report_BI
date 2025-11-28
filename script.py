
import os
import logging
import pandas as pd
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

    # 7. Xử lý dữ liệu từ 4 processors
    print("\n" + "="*60)
    print("🔄 XỬ LÝ DỮ LIỆU")
    print("="*60 + "\n")
    
    # Process 3G Ericsson
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
    print("📊 Processing 3G ZTE...")
    processor_3g_zte = ZTE3GProcessor(download_folder=DOWNLOAD_FOLDER)
    processor_3g_zte.load_all_3g_zte_data()
    processor_3g_zte.merge_final_result()
    processor_3g_zte.standardize_columns()
    processor_3g_zte.clean_data()
    df_3g_zte_site = processor_3g_zte.aggregate_by_site()
    print(f"✅ 3G ZTE: {len(df_3g_zte_site):,} sites\n")
    
    # Process 4G Ericsson
    print("📊 Processing 4G Ericsson...")
    processor_4g_eric = Ericsson4GProcessor(download_folder=DOWNLOAD_FOLDER)
    processor_4g_eric.load_all_4g_ericsson_data()
    processor_4g_eric.merge_final_result()
    processor_4g_eric.standardize_columns()
    processor_4g_eric.clean_data()
    df_4g_eric_site = processor_4g_eric.aggregate_by_site()
    print(f"✅ 4G Ericsson: {len(df_4g_eric_site):,} sites\n")
    
    # Process 4G ZTE
    print("📊 Processing 4G ZTE...")
    processor_4g_zte = ZTE4GProcessor(download_folder=DOWNLOAD_FOLDER)
    processor_4g_zte.load_all_4g_zte_data()
    processor_4g_zte.merge_final_result()
    processor_4g_zte.standardize_columns()
    processor_4g_zte.clean_data()
    df_4g_zte_site = processor_4g_zte.aggregate_by_site()
    print(f"✅ 4G ZTE: {len(df_4g_zte_site):,} sites\n")
    
    # 8. Merge 3G data (Ericsson + ZTE)
    print("🔗 Merging 3G data...")
    df_3g_site = pd.concat([df_3g_eric_site, df_3g_zte_site], ignore_index=True)
    print(f"✅ 3G Combined: {len(df_3g_site):,} sites\n")
    
    # 9. Merge 4G data (Ericsson + ZTE)
    print("🔗 Merging 4G data...")
    df_4g_site = pd.concat([df_4g_eric_site, df_4g_zte_site], ignore_index=True)
    print(f"✅ 4G Combined: {len(df_4g_site):,} sites\n")
    
    # 10. Merge 3G + 4G data
    print("🔗 Merging 3G + 4G data...")
    df_site_data = pd.merge(
        df_3g_site,
        df_4g_site,
        on='SiteID',
        how='outer'
    ).fillna(0)
    print(f"✅ Site Data: {len(df_site_data):,} sites\n")
    
    # 11. Load SiteLocation and add location data
    print("📍 Adding location data...")
    site_location_path = Path(__file__).parent / "SiteLocation.csv"
    df_location = pd.read_csv(site_location_path, usecols=['Site_ID', 'Long', 'Lat'])
    
    # Add Date column (yesterday)
    yesterday = datetime.now() - timedelta(days=1)
    df_site_data['Date'] = yesterday.strftime('%Y-%m-%d')
    
    # Lookup Long and Lat
    location_dict_long = dict(zip(df_location['Site_ID'], df_location['Long']))
    location_dict_lat = dict(zip(df_location['Site_ID'], df_location['Lat']))
    
    df_site_data['Long'] = df_site_data['SiteID'].map(location_dict_long).fillna(0)
    df_site_data['Lat'] = df_site_data['SiteID'].map(location_dict_lat).fillna(0)
    
    # Reorder columns
    final_columns = [
        'Date', 'SiteID', 'Long', 'Lat',
        '3G_User', '3G_Speed', '3G_Voice', '3G_Data',
        '4G_User', '4G_Speed', '4G_Voice', '4G_Data'
    ]
    df_site_data = df_site_data[final_columns]
    print(f"✅ Added location data\n")
    
    # 12. Save to Aggregate.xlsx
    print("💾 Saving to Aggregate.xlsx...")
    aggregate_file = Path(__file__).parent / "Aggregate.xlsx"
    
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
    
    # Save to Excel
    df_combined.to_excel(aggregate_file, index=False)
    print(f"✅ Saved to {aggregate_file.name}\n")
    
    print("="*60)
    print("🎉 DATA PROCESSING COMPLETED!")
    print("="*60)


# Chạy script

if __name__ == "__main__":
    main()
