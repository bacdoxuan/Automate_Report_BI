"""
Module to extract all ZIP files in the downloads folder.
"""
import os
import sys
import zipfile
from pathlib import Path

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


def extract_all_zips(download_folder: str) -> dict:
    """
    Find and extract all .zip files in the specified folder.
    
    Args:
        download_folder: Path to the folder containing zip files
        
    Returns:
        dict: Summary with 'total_zips', 'extracted', 'failed', and 'details' list
    """
    download_path = Path(download_folder)
    
    if not download_path.exists():
        print(f"❌ Thư mục không tồn tại: {download_folder}")
        return {
            'total_zips': 0,
            'extracted': 0,
            'failed': 0,
            'details': []
        }
    
    # Find all .zip files
    zip_files = list(download_path.glob("*.zip"))
    
    if not zip_files:
        print(f"ℹ️ Không tìm thấy file .zip trong thư mục: {download_folder}")
        return {
            'total_zips': 0,
            'extracted': 0,
            'failed': 0,
            'details': []
        }
    
    print(f"📦 Tìm thấy {len(zip_files)} file .zip")
    print(f"📂 Thư mục giải nén: {download_folder}\n")
    
    extracted_count = 0
    failed_count = 0
    details = []
    
    for zip_path in zip_files:
        try:
            print(f"🔓 Đang giải nén: {zip_path.name}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get list of files in the zip
                file_list = zip_ref.namelist()
                
                # Extract to the same downloads folder
                zip_ref.extractall(download_path)
                
                print(f"   ✅ Giải nén thành công: {len(file_list)} file")
                print(f"      📄 Files: {', '.join(file_list[:3])}" + 
                      (f" ... và {len(file_list) - 3} file khác" if len(file_list) > 3 else ""))
                
                extracted_count += 1
                details.append({
                    'zip_file': zip_path.name,
                    'status': 'success',
                    'files_extracted': len(file_list),
                    'file_list': file_list
                })
                
        except zipfile.BadZipFile:
            print(f"   ❌ Lỗi: File không phải định dạng ZIP hợp lệ")
            failed_count += 1
            details.append({
                'zip_file': zip_path.name,
                'status': 'failed',
                'error': 'Bad ZIP file'
            })
            
        except Exception as e:
            print(f"   ❌ Lỗi khi giải nén: {str(e)}")
            failed_count += 1
            details.append({
                'zip_file': zip_path.name,
                'status': 'failed',
                'error': str(e)
            })
    
    # Summary
    print(f"\n📊 Tổng kết giải nén:")
    print(f"  📦 Tổng số file .zip: {len(zip_files)}")
    print(f"  ✅ Giải nén thành công: {extracted_count}")
    if failed_count > 0:
        print(f"  ❌ Thất bại: {failed_count}")
    
    return {
        'total_zips': len(zip_files),
        'extracted': extracted_count,
        'failed': failed_count,
        'details': details
    }


def extract_and_cleanup_zips(download_folder: str, delete_after_extract: bool = False) -> dict:
    """
    Extract all zip files and optionally delete them after extraction.
    
    Args:
        download_folder: Path to the folder containing zip files
        delete_after_extract: If True, delete zip files after successful extraction
        
    Returns:
        dict: Summary with extraction results
    """
    result = extract_all_zips(download_folder)
    
    if delete_after_extract and result['extracted'] > 0:
        print(f"\n🗑️ Đang xóa các file .zip đã giải nén...")
        download_path = Path(download_folder)
        deleted_count = 0
        
        for detail in result['details']:
            if detail['status'] == 'success':
                zip_path = download_path / detail['zip_file']
                try:
                    zip_path.unlink()
                    print(f"  ✅ Đã xóa: {detail['zip_file']}")
                    deleted_count += 1
                except Exception as e:
                    print(f"  ❌ Không thể xóa {detail['zip_file']}: {str(e)}")
        
        print(f"🗑️ Đã xóa {deleted_count}/{result['extracted']} file .zip")
    
    return result


if __name__ == "__main__":
    # Test the module
    DOWNLOAD_FOLDER = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "downloads"
    )
    
    print("=" * 60)
    print("🚀 EXTRACT ZIPPY - ZIP File Extractor")
    print("=" * 60 + "\n")
    
    # Extract without deleting zip files
    extract_all_zips(DOWNLOAD_FOLDER)
    
    # Uncomment below to extract and delete zip files after extraction
    # extract_and_cleanup_zips(DOWNLOAD_FOLDER, delete_after_extract=True)
