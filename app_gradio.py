# =============================================================================
# app_gradio.py
#
# Gradio-based web UI for the Automated BI Report Generator.
# Features: manual script execution, job scheduling with APScheduler,
# schedule management (CRUD), execution history tracking, and log viewing.
# Integrates with scheduler_db.py for persistent schedule storage in SQLite.
# =============================================================================

import gradio as gr
import pandas as pd
import os
import glob
import subprocess
import sys
from datetime import datetime
import atexit
from typing import Dict, Union, List, Any, Tuple
import sqlite3

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Import a new DB module
import scheduler_db

# --- Global objects ---
LOG_DIR = "Log"
scheduler = BackgroundScheduler(timezone="Asia/Ho_Chi_Minh")


# --- Core Functions ---
def get_log_files():
    """Retrieve list of log files from Log directory, sorted by modification time (newest first)."""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_files = glob.glob(os.path.join(LOG_DIR, "*.txt"))
    log_files.sort(key=os.path.getmtime, reverse=True)
    return [os.path.basename(f) for f in log_files]

def get_python_scripts():
    """Get list of Python scripts in current directory, prioritizing script.py at the top."""
    files = glob.glob("*.py")
    # Prioritize script.py at the beginning
    if "script.py" in files:
        files.remove("script.py")
        files.insert(0, "script.py")
    return files

def view_log_file(log_filename):
    """Read and return contents of a log file. Returns error message if file cannot be read."""
    if not log_filename:
        return "Vui lòng chọn một file log để xem."
    log_path = os.path.join(LOG_DIR, log_filename)
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        return f"Lỗi khi đọc file: {e}"

def run_script_manual(skip_email, script_path="script.py", process_date=None):
    """Execute a Python script manually via subprocess and return immediate UI feedback.
    Args:
        skip_email (bool): If True, append --skip-email flag to skip email download.
        script_path (str): Path to the Python script to run (default: script.py).
        process_date (str): Optional date in YYYY-MM-DD format for specific date processing.
    Returns:
        str: Status message confirming task start or error details.
    """
    if not script_path:
        script_path = "script.py"
    
    command = [sys.executable, script_path]
    if skip_email:
        command.append("--skip-email")
        
    if process_date:
        # Validate date format YYYY-MM-DD
        try:
            datetime.strptime(process_date, "%Y-%m-%d")
            command.extend(["--process-date", process_date])
        except ValueError:
            return "❌ Lỗi: Định dạng ngày không hợp lệ. Vui lòng dùng YYYY-MM-DD."

    try:
        subprocess.Popen(command)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = "Chỉ xử lý file" if skip_email else "Toàn bộ quy trình"
        date_msg = f" (Ngày: {process_date})" if process_date else " (Ngày: Hôm qua)"
        return f"[{timestamp}] Đã bắt đầu chạy tác vụ ({script_path}){date_msg}. Chế độ: {mode}. Xem tab 'Xem Logs' để theo dõi chi tiết."
    except Exception as e:
        return f"Lỗi khi bắt đầu tác vụ: {e}"

def run_scheduled_job(schedule_id: int, skip_email: bool):
    """Execute a scheduled job script in a background thread, log outcome to database.
    Retrieves script path from schedule info, runs via subprocess, and records
    execution status (OK/NOK) and details in run_history table.
    Args:
        schedule_id (int): ID of the schedule in the database.
        skip_email (bool): If True, append --skip-email flag.
    """
    # Retrieve schedule info to determine which script to run
    sched_info = scheduler_db.get_schedule(schedule_id)
    script_path = sched_info.get('script_path', 'script.py') if sched_info else "script.py"

    command = [sys.executable, script_path]
    if skip_email:
        command.append("--skip-email")
    try:
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            encoding='utf-8'
        )
        if process.returncode == 0:
            status = "OK"
            details = "Tác vụ hoàn thành thành công."
        else:
            status = "NOK"
            details = f"Lỗi khi chạy {script_path}. Stderr: {process.stderr[-500:]}"
        
        scheduler_db.log_run(schedule_id, status, details)
    except Exception as e:
        scheduler_db.log_run(schedule_id, "NOK", f"Lỗi nghiêm trọng khi khởi chạy job: {e}")


# --- Scheduler and DB Interaction Logic ---
def add_job_to_scheduler(schedule: dict):
    """Add a schedule from database to APScheduler using CronTrigger.
    Creates a cron job that will execute run_scheduled_job() at specified time/frequency.
    Args:
        schedule (dict): Schedule dict containing id, run_time, frequency, day_of_week, skip_email.
    """
    job_id = f"db_job_{schedule['id']}"
    try:
        hour, minute = map(int, schedule['run_time'].split(':'))
        trigger_args = {'hour': hour, 'minute': minute}
        if schedule['frequency'] == "Hàng tuần":
            trigger_args['day_of_week'] = schedule['day_of_week']

        trigger = CronTrigger(**trigger_args)
        scheduler.add_job(
            run_scheduled_job,
            trigger=trigger,
            id=job_id,
            args=[schedule['id'], schedule['skip_email']],
            replace_existing=True
        )
    except Exception as e:
        print(f"Error adding job {job_id} to scheduler: {e}")

def remove_job_from_scheduler(schedule_id: int):
    """Remove a scheduled job from APScheduler by schedule ID.
    Args:
        schedule_id (int): The database schedule ID.
    """
    job_id = f"db_job_{schedule_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

def sync_scheduler_from_db():
    """Reload all active schedules from database into APScheduler.
    Clears existing jobs and recreates them from database records.
    """
    print("Syncing scheduler from database...")
    scheduler.remove_all_jobs()
    schedules = scheduler_db.get_all_schedules()
    for s in schedules:
        if s['is_active']:
            add_job_to_scheduler(s)
    print(f"Scheduler synced. {len(scheduler.get_jobs())} jobs are active.")

def load_schedules_as_dataframe():
    """Convert schedules from database into a pandas DataFrame for UI display.
    Returns empty DataFrame with columns if no schedules exist.
    """
    schedules = scheduler_db.get_all_schedules()
    if not schedules:
        return pd.DataFrame(columns=["ID", "Tên Lịch", "Script", "Tần Suất", "Ngày/Giờ Chạy", "Bỏ qua Email", "Trạng Thái"])
    
    df_data = []
    for s in schedules:
        run_details = f"{s['day_of_week'] if s['day_of_week'] else ''} @ {s['run_time']}".strip()
        # Xử lý trường hợp script_path có thể chưa có trong các bản ghi cũ
        script_name = s.get('script_path', 'script.py')
        
        df_data.append({
            "ID": s['id'], "Tên Lịch": s['job_name'], "Script": script_name, "Tần Suất": s['frequency'],
            "Ngày/Giờ Chạy": run_details, "Bỏ qua Email": "Có" if s['skip_email'] else "Không",
            "Trạng Thái": "Hoạt động" if s['is_active'] else "Dừng"
        })
    return pd.DataFrame(df_data)

def get_schedule_choices():
    """Get list of schedule names with IDs for Gradio dropdown component.
    Returns:
        List[str]: List of formatted strings: 'Schedule Name (ID: 123)'.
    """
    schedules = scheduler_db.get_all_schedules()
    return [f"{s['job_name']} (ID: {s['id']})" for s in schedules]

def handle_add_schedule(name, freq, day, time, skip, active, script_path):
    """Add a new schedule to the database and optionally add to scheduler.
    Returns status message, updated DataFrame, and updated dropdown choices.
    """
    if not name or not time:
        return "Tên lịch và thời gian chạy không được để trống.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())
    try:
        day_map = {"Thứ 2": "mon", "Thứ 3": "tue", "Thứ 4": "wed", "Thứ 5": "thu", "Thứ 6": "fri", "Thứ 7": "sat", "Chủ nhật": "sun"}
        day_str = day_map.get(day) if freq == "Hàng tuần" else None
        
        # Sử dụng mặc định script.py nếu không chọn
        if not script_path:
            script_path = "script.py"

        schedule_id = scheduler_db.add_schedule(name, freq, day_str, time, skip, active, script_path)
        if active:
            schedule = scheduler_db.get_schedule(schedule_id)
            if schedule: add_job_to_scheduler(schedule)
        
        return f"Đã thêm lịch '{name}' chạy script '{script_path}'.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())
    except sqlite3.IntegrityError:
        return f"Lỗi: Tên lịch '{name}' đã tồn tại.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())
    except Exception as e:
        return f"Lỗi: {e}", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())

# --- Delete Confirmation Handlers ---
def prompt_delete(schedule_choice: str) -> List[Any]:
    """Show delete confirmation dialog. Returns updated visibility states for UI groups and confirmation message."""
    if not schedule_choice:
        return [gr.update(), gr.update(), gr.update(value="Vui lòng chọn một lịch để xóa.")]
    
    return [
        gr.update(visible=False), # Hide management buttons
        gr.update(visible=True),  # Show confirmation buttons
        gr.update(value=f"Bạn có chắc chắn muốn xóa lịch '{schedule_choice}' không? Hành động này không thể hoàn tác.")
    ]

def cancel_delete() -> List[Any]:
    """Hide delete confirmation dialog and return UI to management view."""
    return [
        gr.update(visible=True),  # Show management buttons
        gr.update(visible=False), # Hide confirmation buttons
        gr.update(value="")
    ]

def handle_delete_schedule(schedule_choice: str) -> Tuple[Any, ...]:
    """Delete schedule from database and APScheduler, return updated UI components.
    Parses schedule ID from choice string and removes from both database and scheduler.
    """
    if not schedule_choice:
        msg = "Lỗi: Không có lịch nào được chọn để xóa."
    else:
        try:
            schedule_id = int(schedule_choice.split("ID: ")[1].strip(")"))
            remove_job_from_scheduler(schedule_id)
            scheduler_db.delete_schedule(schedule_id)
            msg = f"Đã xóa lịch '{schedule_choice}'."
        except (IndexError, ValueError) as e:
            msg = f"Lỗi khi xử lý lựa chọn: {e}"

    return (
        msg,
        load_schedules_as_dataframe(),
        gr.Dropdown(choices=get_schedule_choices(), value=None),
        gr.update(visible=True),  # Show management buttons
        gr.update(visible=False)  # Hide confirmation buttons
    )

def handle_toggle_status(schedule_choice: str, new_status: bool):
    """Activate or deactivate a schedule and sync with APScheduler.
    Args:
        schedule_choice (str): Formatted string 'Schedule Name (ID: 123)'.
        new_status (bool): True to activate, False to deactivate.
    Returns:
        Tuple of status message and updated schedules DataFrame.
    """
    if not schedule_choice:
        return "Vui lòng chọn một lịch để thay đổi.", load_schedules_as_dataframe()
    schedule_id = int(schedule_choice.split("ID: ")[1].strip(")"))
    scheduler_db.update_schedule_status(schedule_id, new_status)
    if new_status:
        schedule = scheduler_db.get_schedule(schedule_id)
        if schedule: add_job_to_scheduler(schedule)
        msg = "Đã kích hoạt lịch."
    else:
        remove_job_from_scheduler(schedule_id)
        msg = "Đã dừng lịch."
    return msg, load_schedules_as_dataframe()

def handle_view_history(schedule_choice: str):
    """Retrieve and display execution history for a schedule.
    Returns status message and DataFrame of run history entries.
    """
    if not schedule_choice:
        return "Vui lòng chọn một lịch để xem lịch sử.", gr.DataFrame(visible=False)
    
    schedule_id = int(schedule_choice.split("ID: ")[1].strip(")"))
    history = scheduler_db.get_run_history(schedule_id)
    
    if not history:
        return f"Không có lịch sử chạy cho lịch (ID: {schedule_id}).", gr.DataFrame(visible=False)

    df = pd.DataFrame(history)
    df.rename(columns={"run_timestamp": "Thời gian chạy", "status": "Kết quả", "details": "Chi tiết"}, inplace=True)
    return f"Hiển thị lịch sử cho lịch (ID: {schedule_id}).", gr.DataFrame(value=df, visible=True)


# --- Gradio UI ---
with gr.Blocks(title="Automate Report BI - Dashboard") as automate_report_server:
    with gr.Row(elem_id="app-header"):
        gr.Markdown(
            '<h1 style="margin: 0;">Hệ thống quản lý báo cáo tự động - VNM</h1>'
        )

    with gr.Tabs():
        with gr.TabItem("▶️ Chạy thủ công"):
            gr.Markdown("## Chạy tác vụ ngay lập tức")
            
            with gr.Row():
                manual_script_dropdown = gr.Dropdown(
                    label="Chọn file Script", 
                    choices=get_python_scripts(), 
                    value="script.py" if "script.py" in get_python_scripts() else None,
                    allow_custom_value=True
                )
                refresh_scripts_btn = gr.Button("🔄 Làm mới danh sách file script", size="md", scale=1)
            
            with gr.Row():
                manual_date_input = gr.Textbox(
                    label="Ngày xử lý (YYYY-MM-DD)", 
                    placeholder="Để trống: Mặc định lấy ngày Hôm qua. Chạy dữ liệu với 1 ngày cụ thể, điền thông tin dạng YYYY-MM-DD, ví dụ: 2024-01-01",
                    value=""
                )

            with gr.Row():
                run_full_button = gr.Button("🚀 Chạy với file script đã chọn")
                run_skip_email_button = gr.Button("⏩ Chạy chỉ xử lý file (Bỏ qua download email attachment)")
            manual_run_status = gr.Textbox(label="Trạng thái", interactive=False)

            refresh_scripts_btn.click(
                lambda: gr.Dropdown(choices=get_python_scripts()), 
                None, 
                manual_script_dropdown
            )

        with gr.TabItem("📅 Lịch chạy"):
            with gr.Tabs():
                with gr.TabItem("Thêm mới lịch chạy"):
                    gr.Markdown("### Thêm lịch mới")
                    add_name = gr.Textbox(label="Tên lịch (duy nhất)")
                    
                    with gr.Row():
                        add_script = gr.Dropdown(
                            label="File Script chạy",
                            choices=get_python_scripts(),
                            value="script.py" if "script.py" in get_python_scripts() else None,
                            allow_custom_value=True
                        )
                        refresh_add_script_btn = gr.Button("🔄 Làm mới danh sách file script", size="md", scale=1)

                    with gr.Row():
                        add_freq = gr.Radio(["Hàng ngày", "Hàng tuần"], label="Tần suất", value="Hàng ngày")
                        add_dow = gr.Dropdown(["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ nhật"], label="Ngày trong tuần", value="Thứ 2", visible=False)
                    add_time = gr.Textbox(label="Thời gian chạy (HH:MM)", placeholder="Ví dụ: 08:30")
                    add_skip = gr.Checkbox(label="Chỉ xử lý file (bỏ qua email)")
                    add_active = gr.Checkbox(label="Kích hoạt ngay sau khi thêm", value=True)
                    add_button = gr.Button("Thêm lịch mới", variant="primary")
                    
                    add_freq.change(lambda f: gr.update(visible=f == "Hàng tuần"), add_freq, add_dow)
                    refresh_add_script_btn.click(
                        lambda: gr.Dropdown(choices=get_python_scripts()),
                        None,
                        add_script
                    )
                    
                with gr.TabItem("Quản lý lịch chạy đã có"):
                    gr.Markdown("### Danh sách và quản lý lịch chạy")
                    schedules_df = gr.DataFrame(load_schedules_as_dataframe, wrap=True, label="Danh sách lịch chạy")
                    gr.Markdown("### Quản lý")
                    sched_choice = gr.Dropdown(choices=get_schedule_choices(), label="Chọn lịch để quản lý")
                    
                    with gr.Group() as manage_buttons_group:
                        with gr.Row():
                            activate_button = gr.Button("✅ Kích hoạt")
                            deactivate_button = gr.Button("⛔ Dừng")
                        with gr.Row():
                            history_button = gr.Button("📜 Xem Lịch sử")
                            delete_button = gr.Button("🗑️ Xóa", variant="stop")

                    with gr.Group(visible=False) as confirm_delete_group:
                        confirm_delete_text = gr.Markdown()
                        with gr.Row():
                            confirm_delete_button = gr.Button("Có, xóa", variant="stop")
                            cancel_delete_button = gr.Button("Hủy")

                    history_df = gr.DataFrame(visible=False, label="Lịch sử chạy")

            manage_status = gr.Textbox(label="Kết quả", interactive=False)

        with gr.TabItem("📄 Xem Logs"):
            gr.Markdown("## Xem lại lịch sử chạy")
            with gr.Row():
                log_files_dropdown = gr.Dropdown(label="Chọn file Log", choices=get_log_files(), value=get_log_files()[0] if get_log_files() else None)
                refresh_logs_button = gr.Button("🔄 Làm mới")
            log_content_display = gr.Textbox(label="Nội dung Log", lines=20, interactive=False, autoscroll=True)

        with gr.TabItem("☎️ Liên hệ"):
            gr.Markdown(
                """
                ## Thông tin liên hệ
                Mọi thắc mắc và hỗ trợ xin liên hệ:
                - **Tác giả:** Đỗ Xuân Bắc
                - **Số điện thoại:** 0925007589
                - **Email:** bac.dx@vietnamobile.com.vn
                """
            )

    # --- Event Handlers ---
    # Manual run handlers updated to pass script path AND process date
    run_full_button.click(
        fn=run_script_manual, 
        inputs=[gr.State(False), manual_script_dropdown, manual_date_input], 
        outputs=[manual_run_status]
    )
    run_skip_email_button.click(
        fn=run_script_manual, 
        inputs=[gr.State(True), manual_script_dropdown, manual_date_input], 
        outputs=[manual_run_status]
    )
    
    add_button.click(
        fn=handle_add_schedule, 
        inputs=[add_name, add_freq, add_dow, add_time, add_skip, add_active, add_script], 
        outputs=[manage_status, schedules_df, sched_choice]
    )

    activate_button.click(fn=lambda c: handle_toggle_status(c, True), inputs=[sched_choice], outputs=[manage_status, schedules_df])
    deactivate_button.click(fn=lambda c: handle_toggle_status(c, False), inputs=[sched_choice], outputs=[manage_status, schedules_df])
    history_button.click(fn=handle_view_history, inputs=[sched_choice], outputs=[manage_status, history_df])

    # Delete confirmation flow
    delete_button.click(
        fn=prompt_delete, 
        inputs=[sched_choice], 
        outputs=[manage_buttons_group, confirm_delete_group, manage_status]
    )
    cancel_delete_button.click(
        fn=cancel_delete, 
        inputs=[], 
        outputs=[manage_buttons_group, confirm_delete_group, manage_status]
    )
    confirm_delete_button.click(
        fn=handle_delete_schedule, 
        inputs=[sched_choice], 
        outputs=[manage_status, schedules_df, sched_choice, manage_buttons_group, confirm_delete_group]
    )

    log_files_dropdown.change(view_log_file, log_files_dropdown, log_content_display)
    refresh_logs_button.click(lambda: (gr.Dropdown(choices=get_log_files()), gr.Textbox(value="")), [], [log_files_dropdown, log_content_display])
    automate_report_server.load(view_log_file, log_files_dropdown, log_content_display)

# --- Startup and Shutdown ---
print("=" * 60)
print("Get scheduler from DB...")
try:
    sync_scheduler_from_db()
    print("Get scheduler from DB successfully.")
except Exception as e:
    print(f"Error getting scheduler from DB: {e}")

print("\n" + "=" * 60)
print("Start scheduler...")
try:
    scheduler.start()
    print("Scheduler started successfully.")
except Exception as e:
    print(f"Error starting scheduler: {e}")

print("\n" + "=" * 60)
print("Register shutdown...")
try:
    atexit.register(lambda: scheduler.shutdown())
    print("Register shutdown successfully.")
except Exception as e:
    print(f"Error registering shutdown: {e}")

print("\n" + "=" * 60)


if __name__ == "__main__":
    automate_report_server.launch()