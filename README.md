# Automated BI Report Generator (Vietnamobile)

Tự động hóa ETL báo cáo KPI mạng cho PowerBI: tải file dữ liệu 3G/4G (Ericsson + ZTE) từ email Microsoft Exchange, giải nén, xử lý (tính busy hour, chuẩn hóa, tổng hợp theo site) và xuất ra file `Aggregate.xlsx` duy nhất.

Kèm dashboard web (Gradio) để chạy thủ công, đặt lịch tự động (APScheduler + SQLite) và xem log.

---

## Kiến trúc tổng quan

```
app_gradio.py (Web UI + APScheduler) ── subprocess ──> script.py (orchestrator)
                                                            ├─ exchange_lib.py     Kết nối EWS (NTLM), tìm & tải email
                                                            ├─ extract_zippy.py    Giải nén file ZIP trong downloads/
                                                            ├─ processing_*.py     4 processors (3G/4G × Ericsson/ZTE)
                                                            ├─ notification.py     Email báo ✅/❌ kèm file log
                                                            └─ Aggregate.xlsx      Kết quả cuối (chunked write, giữ 30 ngày)
```

### Luồng xử lý của `script.py`

1. **Kết nối Exchange** – NTLM, delegate access, không dùng autodiscover.
2. **Dọn sạch thư mục `downloads/`** – xóa toàn bộ rồi tạo lại để đảm bảo dữ liệu mới.
3. **Tải email – Pass 1 (Ericsson, mailbox cá nhân):**
   - Người gửi: `bac.dx@vietnamobile.com.vn`
   - 4 subject: `Automate_3G_Throughput`, `Automate_3G_Traffic_User`, `Automate_VoLTE_Traffic_Ericsson`, `Automate_North_LTE_Traffic_Data`
4. **Tải email – Pass 2 (ZTE, mailbox chia sẻ):**
   - Người gửi: `vnm.performance.reporting@vietnamobile.com.vn`
   - 8 subject dạng `[EXTERNAL]Task name:Automate_3G/4G_ZTE_Traffic/User_TP_EMS1/2_WD/BH`
5. **Giải nén** mọi file ZIP trong `downloads/`.
6. **Xử lý dữ liệu** bằng 4 processors (xem bên dưới), mỗi processor ra DataFrames cấp site: `SiteID, {3G|4G}_User, {3G|4G}_Speed, {3G|4G}_Voice, {3G|4G}_Data`.
7. **Kết hợp:** concat Ericsson + ZTE cho từng tầng 3G/4G → merge outer theo `SiteID` (giá trị thiếu = 0) → thêm cột `Date`.
8. **Lưu `Aggregate.xlsx`** (thư mục cha của project): append vào dữ liệu cũ, chỉ giữ **30 ngày gần nhất**, ghi bằng openpyxl `write_only` theo chunk 10.000 dòng (ổn định cho ~180k dòng), ghi file tạm rồi replace atomic.
9. **Sao chép** kết quả sang thư mục backup (`PATH_TO_COPY` trong `script.py`).
10. **Gửi email thông báo** thành công/thất bại kèm file log.

### Logic ngày

- Dữ liệu của ngày **T** nằm trong email gửi ngày **T+1**.
- `process_date` (mặc định: hôm qua) → `email_search_date = process_date + 1 ngày`.
- Tìm email theo đúng ngày (00:00:00–23:59:59) bằng múi giờ `Asia/Ho_Chi_Minh`.

---

## Cấu trúc thư mục

| File / Thư mục | Vai trò |
|---|---|
| `app_gradio.py` | Entry point chính – Web UI (Gradio) + scheduler (APScheduler). |
| `script.py` | Orchestrator pipeline ETL, có thể chạy độc lập từ CLI. Chứa toàn bộ khối CẤU HÌNH (subject, sender, folder, ngày…). |
| `downloademailonly.py` | Chỉ tải email + giải nén, không xử lý dữ liệu (dùng khi test/thu thập thủ công). |
| `testconnection.py` | Kiểm tra kết nối Exchange với cấu hình `.env` hiện tại. |
| `exchange_lib.py` | `get_exchange_account()`, `find_and_download_emails()`, `send_email()`. |
| `extract_zippy.py` | `extract_all_zips()`, `extract_and_cleanup_zips()`. |
| `notification.py` | Lớp `Notifier` – email báo kết quả (thành công/thất bại) kèm log. |
| `scheduler_db.py` | Toàn bộ thao tác SQLite: bảng `schedules` và `schedule_runs`. Init tự động khi import. |
| `processing_3G_Ericsson.py` | `Ericsson3GProcessor` – XLSX, multi-sheet, busy-hour lookup. |
| `processing_3G_ZTE.py` | `ZTE3GProcessor` – 4 CSV EMS1/EMS2, concat + merge. |
| `processing_4G_Ericsson.py` | `Ericsson4GProcessor` – XLSX, multi-sheet, busy-hour lookup. |
| `processing_4G_ZTE.py` | `ZTE4GProcessor` – 4 CSV EMS1/EMS2, concat + merge. |
| `.env` | Credentials (không commit). |
| `schedules.db` | SQLite lưu lịch chạy + lịch sử (tạo tự động). |
| `SiteLocation.csv` | File tham chiếu vị trí site (tương tác Long/Lat hiện đang bị bỏ qua trong pipeline, xem *Ghi chú*). |
| `downloads/` | Nơi lưu attachment tải về + file sau giải nén (xóa & tạo lại mỗi lần chạy). |
| `Log/` | Log chạy: `log_YYYY-MM-DD.txt` (redirect stdout/stderr). |
| `Aggregate.xlsx` | Kết quả cuối cùng (ở thư mục **cha** của project), đầu vào cho PowerBI. |

---

## Schema dữ liệu cuối cùng

Cột output của `Aggregate.xlsx`:

```
Date, Site, 3G Sub, 3G Speed, 3G Voice traffic, 3G Data traffic,
4G Sub, 4G Speed, 4G Voice traffic, 4G Data traffic
```

Quy tắc tổng hợp cấp site (áp dụng chung cho 4 processors):
- `SiteID` = 6 ký tự đầu của Cell ID (vị trí 1–6, 0-based).
- **User / Voice / Data** = tổng (sum) các cell trong site.
- **Speed** = trung bình (mean) các cell trong site.
- Dọn dữ liệu: loại các dòng có Data = 0 trước khi tổng hợp.

---

## 4 Modules xử lý dữ liệu

Tất cả processors tuân thủ workflow chuẩn: **Import → (Transform) → Merge → Standardize → Clean → Aggregate**.

| Processor | Input | Điểm xử lý chính |
|---|---|---|
| `Ericsson3GProcessor` | `Automate_3G_Throughput.xlsx` (sheet `User_TP_DL`), `Automate_3G_Traffic_User.xlsx` (sheets `Voice_Erlang`, `Data_MB`, `HS_User`) | Max user + busy hour index từ `HS_User`; lookup 3G speed tại giờ busy; sum voice/data 24h. |
| `ZTE3GProcessor` | 4 CSV (pattern `Automate_3G_ZTE_Traffic_EMS1/2_WD_*.csv`, `Automate_3G_ZTE_User_TP_EMS1/2_BH_*.csv`) | Concat EMS1 + EMS2, merge traffic + user throughput theo `Cell Name`. |
| `Ericsson4GProcessor` | `Automate_North_LTE_Traffic_Data.xlsx` (sheets `Data_MB`, `UE_Active_DL`, `UE_TP_DL`), `Automate_VoLTE_Traffic_Ericsson.xlsx` (sheet `VoLTE_Traffic`) | Tổng data 24h; max UE active + busy hour; throughput tại giờ max UE; sum VoLTE. |
| `ZTE4GProcessor` | 4 CSV (pattern `Automate_4G_ZTE_Traffic_EMS1/2_WD_*.csv`, `Automate_4G_ZTE_User_TP_EMS1/2_BH_*.csv`) | Concat EMS1 + EMS2, merge traffic + user throughput theo `Cell Name`. |

Lưu ý kỹ thuật:
- Dùng `safe_idxmax_axis1()` (row-wise idxmax an toàn khi hàng toàn NA – tương thích pandas 3.x).
- File ZTE được tìm bằng **glob pattern** vì tên file có hậu tố động.

---

## Cài đặt

Yêu cầu: Python 3.8+ (target Windows).

```bash
pip install -r requirements.txt
```

`requirements.txt`: `pandas`, `openpyxl`, `exchangelib`, `python-dotenv`, `gradio`, `apscheduler`.

### Cấu hình `.env`

Tạo file `.env` ở thư mục gốc:

```ini
EMAIL_ADDRESS=your_email@vietnamobile.com.vn
EMAIL_PASSWORD=your_password
EXCHANGE_SERVER=your-ews-server
EXCHANGE_DOMAIN=YOURDOMAIN
EXCHANGE_USERNAME=your_username
```

### Cấu hình trong `script.py`

Các hằng số có thể chỉnh trực tiếp ở khối **CẤU HÌNH** phía trên `main()`:

| Hằng số | Ý nghĩa |
|---|---|
| `LIST_OF_EMAILS` / `LIST_OF_EMAILS_Z` | Danh sách subject email cần tải (Ericsson / ZTE). |
| `SENDER_EMAIL` / `SENDER_EMAIL_Z` | Lọc theo người gửi (đặt `None` để không lọc). |
| `FOLDER_NAME` / `FOLDER_NAME_Z` | Tên thư mục trong Exchange (mặc định `inbox`). |
| `DOWNLOAD_FOLDER` | Thư mục lưu file tải về (mặc định `downloads`). |
| `PATH_TO_COPY` | Thư mục backup kết quả (đặt `""` để tắt). |
| `ALLOWED_EXTENSIONS` | Giới hạn định dạng file tải về (trống = tất cả). |
| `DAYS_TO_SEARCH` | Số ngày tìm ngược (thường bỏ qua khi có `target_date`). |
| `LOG_LEVEL` | `WARNING` / `INFO` / `DEBUG`. |
| `RESULT_RECEIVER_LIST` | Người nhận email báo kết quả. |

---

## Sử dụng

### 1. Web UI (khuyến nghị)

```bash
python app_gradio.py
```

Mở trình duyệt: `http://127.0.0.1:7860`.

Các tab:
- **▶️ Chạy thủ công** – chọn file script (mặc định `script.py`, có thể nhập tên khác), chọn ngày xử lý (để trống = hôm qua), chạy toàn bộ hoặc bỏ qua download email.
- **📅 Lịch chạy** –
  - *Thêm mới lịch chạy:* tên lịch (duy nhất), script, tần suất (Hàng ngày / Hàng tuần + ngày trong tuần), giờ chạy (HH:MM), skip-email, kích hoạt ngay.
  - *Quản lý lịch chạy đã có:* bảng danh sách, kích hoạt / dừng / xem lịch sử (thời gian, OK/NOK, chi tiết) / xóa (có bước xác nhận).
- **📄 Xem Logs** – chọn và xem nội dung file log trong `Log/`.
- **☎️ Liên hệ** – thông tin hỗ trợ.

Lịch được lưu vào `schedules.db` nên **giữ nguyên sau khi restart ứng dụng**; khi khởi động, `app_gradio.py` tự đồng bộ các lịch đang hoạt động vào APScheduler.

### 2. Command line

```bash
# Chạy full pipeline (download + xử lý), ngày = hôm qua
python script.py

# Chỉ xử lý file có sẵn trong downloads/ (bỏ qua email)
python script.py --skip-email

# Chạy cho một ngày cụ thể
python script.py --process-date 2026-10-25

# Tổ hợp
python script.py --skip-email --process-date 2026-10-25
```

Các công cụ phụ:

```bash
python testconnection.py            # Kiểm tra kết nối Exchange
python downloademailonly.py         # Chỉ tải email + giải nén
python downloademailonly.py --skip-email --process-date 2026-10-25
```

Mỗi processor cũng có thể chạy riêng để test: `python processing_3G_ZTE.py` (cần file đã có trong `downloads/`).

---

## Logging & Notification

- **Log file:** `Log/log_YYYY-MM-DD.txt` (theo ngày xử lý). `sys.stdout`/`sys.stderr` được redirect đồng thời ra console và file.
- **Email báo kết quả** (`notification.py`):
  - Thành công: `[Automate Job Result] ✅ ...` kèm file log.
  - Thất bại: `[Automate Job Result] ❌ ...` kèm bước lỗi, thông báo lỗi và file log. Nếu chưa có kết nối Exchange khi lỗi xảy ra, script sẽ thử kết nối lại để gửi email.

---

## Ghi chú phát triển

- **NTLM on-prem Exchange** – giải pháp này gắn với Exchange server nội bộ, `exchangelib` với `auth_type=NTLM`, `access_type=DELEGATE`, `autodiscover=False`.
- **Dọn folder mỗi lần chạy:** `downloads/` bị xóa hoàn toàn trước khi tải (chủ đích – đảm bảo dữ liệu tươi).
- **`Aggregate.xlsx` giữ 30 ngày:** sau khi append dữ liệu mới, các dòng cũ hơn 30 ngày bị cắt bỏ.
- **Location (Long/Lat):** bước tra `SiteLocation.csv` hiện đang bị comment trong `script.py` (dự định thêm tại hệ thống khác) – không ảnh hưởng schema hiện tại.
- **Mở rộng source dữ liệu:** thêm module `processing_*.py` mới theo mẫu chuẩn (Import → Transform → Standardize → Clean → Aggregate) rồi gọi trong pipeline `script.py`.
- **Không sửa schema DB tay:** mọi thay đổi cấu trúc `schedules.db` phải qua `scheduler_db.py` (có sẵn cơ chế migration).
- **Đường dẫn:** dùng `os.path` / `pathlib`; target platform là Windows.

---

## Xử lý sự cố

| Triệu chứng | Cách xử lý |
|---|---|
| Không kết nối được Exchange | Chạy `python testconnection.py`; kiểm tra `.env`, server (hostname/URL EWS), VPN/firewall, domain\username. |
| Không tìm thấy email | Đối chiếu lại subject trong `LIST_OF_EMAILS`/`LIST_OF_EMAILS_Z`, người gửi, và logic ngày: email của dữ liệu ngày T được gửi ngày T+1. |
| File CSV/ZTE thiếu | Kiểm tra `downloads/` sau giải nén; tên file ZTE có hậu tố động nên processor dùng pattern `*.csv`. |
| Lỗi pandas khi tính busy hour | Bản code hiện tại đã dùng `safe_idxmax_axis1()` cho hàng toàn NA (pandas 3.x). |
| Chạy thất bại từ lịch | Xem tab *Quản lý lịch chạy → Xem Lịch sử* (trường OK/NOK + chi tiết) và file log tương ứng trong `Log/`. |

---

**Liên hệ:** Đỗ Xuân Bắc – 0925007589 – bac.dx@vietnamobile.com.vn
