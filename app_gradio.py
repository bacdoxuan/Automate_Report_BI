import gradio as gr
import pandas as pd
import os
import glob
import subprocess
import sys
from datetime import datetime
import atexit
from typing import Dict, Union
import sqlite3

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Import a new DB module
import scheduler_db

# --- Global objects ---
LOG_DIR = "Log"
scheduler = BackgroundScheduler(timezone="Asia/Ho_Chi_Minh")


# --- Core Functions (unchanged) ---
def get_log_files():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_files = glob.glob(os.path.join(LOG_DIR, "*.txt"))
    log_files.sort(key=os.path.getmtime, reverse=True)
    return [os.path.basename(f) for f in log_files]

def view_log_file(log_filename):
    if not log_filename:
        return "Vui lòng chọn một file log để xem."
    log_path = os.path.join(LOG_DIR, log_filename)
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        return f"Lỗi khi đọc file: {e}"

def run_script(skip_email):
    command = [sys.executable, "script.py"]
    if skip_email:
        command.append("--skip-email")
    try:
        subprocess.Popen(command)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = "Chỉ xử lý file" if skip_email else "Toàn bộ quy trình"
        return f"[{timestamp}] Đã bắt đầu chạy tác vụ. Chế độ: {mode}."
    except Exception as e:
        return f"Lỗi khi bắt đầu tác vụ: {e}"


# --- New Scheduler and DB Interaction Logic ---

def add_job_to_scheduler(schedule: dict):
    """Adds a single job from a schedule dictionary to the APScheduler."""
    job_id = f"db_job_{schedule['id']}"
    try:
        hour, minute = map(int, schedule['run_time'].split(':'))
        trigger_args = {'hour': hour, 'minute': minute}
        if schedule['frequency'] == "Hàng tuần":
            trigger_args['day_of_week'] = schedule['day_of_week']

        trigger = CronTrigger(**trigger_args)
        scheduler.add_job(
            run_script,
            trigger=trigger,
            id=job_id,
            args=[schedule['skip_email']],
            replace_existing=True
        )
    except Exception as e:
        print(f"Error adding job {job_id} to scheduler: {e}")

def remove_job_from_scheduler(schedule_id: int):
    """Removes a job from the scheduler."""
    job_id = f"db_job_{schedule_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

def sync_scheduler_from_db():
    """Loads all active schedules from the DB into the scheduler."""
    print("Syncing scheduler from database...")
    scheduler.remove_all_jobs()
    schedules = scheduler_db.get_all_schedules()
    for s in schedules:
        if s['is_active']:
            add_job_to_scheduler(s)
    print(f"Scheduler synced. {len(scheduler.get_jobs())} jobs are active.")

def load_schedules_as_dataframe():
    """Fetches schedules and formats them for a Gradio DataFrame."""
    schedules = scheduler_db.get_all_schedules()
    if not schedules:
        return pd.DataFrame(columns=["ID", "Tên Lịch", "Tần Suất", "Ngày/Giờ Chạy", "Bỏ qua Email", "Trạng Thái"])
    
    df_data = []
    for s in schedules:
        run_details = f"{s['day_of_week'] if s['day_of_week'] else ''} @ {s['run_time']}".strip()
        df_data.append({
            "ID": s['id'],
            "Tên Lịch": s['job_name'],
            "Tần Suất": s['frequency'],
            "Ngày/Giờ Chạy": run_details,
            "Bỏ qua Email": "Có" if s['skip_email'] else "Không",
            "Trạng Thái": "Hoạt động" if s['is_active'] else "Dừng"
        })
    return pd.DataFrame(df_data)

def get_schedule_choices():
    """Gets a list of schedule names for a dropdown."""
    schedules = scheduler_db.get_all_schedules()
    return [f"{s['job_name']} (ID: {s['id']})" for s in schedules]

def handle_add_schedule(name, freq, day, time, skip, active):
    """Handles UI request to add a new schedule."""
    if not name or not time:
        return "Tên lịch và thời gian chạy không được để trống.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())
    try:
        day_map = {"Thứ 2": "mon", "Thứ 3": "tue", "Thứ 4": "wed", "Thứ 5": "thu", "Thứ 6": "fri", "Thứ 7": "sat", "Chủ nhật": "sun"}
        day_str = day_map.get(day) if freq == "Hàng tuần" else None
        
        schedule_id = scheduler_db.add_schedule(name, freq, day_str, time, skip, active)
        if active:
            schedule = scheduler_db.get_schedule(schedule_id)
            if schedule:
                add_job_to_scheduler(schedule)
        
        return f"Đã thêm lịch '{name}'.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())
    except sqlite3.IntegrityError:
        return f"Lỗi: Tên lịch '{name}' đã tồn tại.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())
    except Exception as e:
        return f"Lỗi: {e}", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())

def handle_delete_schedule(schedule_choice: str):
    """Handles UI request to delete a schedule."""
    if not schedule_choice:
        return "Vui lòng chọn một lịch để xóa.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())
    
    schedule_id = int(schedule_choice.split("ID: ")[1].strip(")"))
    remove_job_from_scheduler(schedule_id)
    scheduler_db.delete_schedule(schedule_id)
    return "Đã xóa lịch.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())

def handle_toggle_status(schedule_choice: str, new_status: bool):
    """Handles UI request to activate/deactivate a schedule."""
    if not schedule_choice:
        return "Vui lòng chọn một lịch để thay đổi.", load_schedules_as_dataframe()

    schedule_id = int(schedule_choice.split("ID: ")[1].strip(")"))
    scheduler_db.update_schedule_status(schedule_id, new_status)
    
    if new_status:
        schedule = scheduler_db.get_schedule(schedule_id)
        if schedule:
            add_job_to_scheduler(schedule)
        msg = "Đã kích hoạt lịch."
    else:
        remove_job_from_scheduler(schedule_id)
        msg = "Đã dừng lịch."

    return msg, load_schedules_as_dataframe()


# --- Gradio UI ---
with gr.Blocks(title="Automate Report BI - Dashboard") as demo:
    gr.Markdown("# Bảng điều khiển tác vụ tự động")

    with gr.Tabs():
        with gr.TabItem("▶️ Chạy thủ công"):
            gr.Markdown("## Chạy tác vụ ngay lập tức")
            with gr.Row():
                run_full_button = gr.Button("🚀 Chạy toàn bộ quy trình")
                run_skip_email_button = gr.Button("⏩ Chạy chỉ xử lý file")
            manual_run_status = gr.Textbox(label="Trạng thái", interactive=False)

        with gr.TabItem("📅 Lịch chạy"):
            gr.Markdown("## Quản lý lịch chạy tự động")
            gr.Markdown("Thêm, xóa, hoặc bật/tắt các lịch chạy tự động.")
            
            # Display Schedules
            schedules_df = gr.DataFrame(load_schedules_as_dataframe, wrap=True, label="Danh sách lịch chạy")
            
            with gr.Row():
                with gr.Group():
                    gr.Markdown("### Thêm lịch mới")
                    add_name = gr.Textbox(label="Tên lịch (duy nhất)")
                    with gr.Row():
                        add_freq = gr.Radio(["Hàng ngày", "Hàng tuần"], label="Tần suất", value="Hàng ngày")
                        add_dow = gr.Dropdown(["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ nhật"], label="Ngày trong tuần", value="Thứ 2", visible=False)
                    add_time = gr.Textbox(label="Thời gian chạy (HH:MM)", placeholder="Ví dụ: 08:30")
                    add_skip = gr.Checkbox(label="Chỉ xử lý file (bỏ qua email)")
                    add_active = gr.Checkbox(label="Kích hoạt ngay sau khi thêm", value=True)
                    add_button = gr.Button("Thêm lịch mới", variant="primary")
                    add_freq.change(lambda f: gr.update(visible=f == "Hàng tuần"), add_freq, add_dow)

                with gr.Group():
                    gr.Markdown("### Quản lý lịch đã có")
                    sched_choice = gr.Dropdown(choices=get_schedule_choices(), label="Chọn lịch để quản lý")
                    with gr.Row():
                        activate_button = gr.Button("✅ Kích hoạt")
                        deactivate_button = gr.Button("⛔ Dừng")
                    delete_button = gr.Button("🗑️ Xóa", variant="stop")
            
            # Status Textbox
            manage_status = gr.Textbox(label="Kết quả", interactive=False)

        with gr.TabItem("📄 Xem Logs"):
            gr.Markdown("## Xem lại lịch sử chạy")
            with gr.Row():
                log_files_dropdown = gr.Dropdown(label="Chọn file Log", choices=get_log_files(), value=get_log_files()[0] if get_log_files() else None)
                refresh_logs_button = gr.Button("🔄 Làm mới")
            log_content_display = gr.Textbox(label="Nội dung Log", lines=20, interactive=False, autoscroll=True)


    # --- Event Handlers ---
    # Manual Run
    run_full_button.click(lambda: run_script(False), [], manual_run_status)
    run_skip_email_button.click(lambda: run_script(True), [], manual_run_status)
    
    # Add Schedule
    add_button.click(
        fn=handle_add_schedule,
        inputs=[add_name, add_freq, add_dow, add_time, add_skip, add_active],
        outputs=[manage_status, schedules_df, sched_choice]
    )

    # Manage Schedule
    activate_button.click(
        fn=lambda choice: handle_toggle_status(choice, True),
        inputs=[sched_choice],
        outputs=[manage_status, schedules_df]
    )
    deactivate_button.click(
        fn=lambda choice: handle_toggle_status(choice, False),
        inputs=[sched_choice],
        outputs=[manage_status, schedules_df]
    )
    delete_button.click(
        fn=handle_delete_schedule,
        inputs=[sched_choice],
        outputs=[manage_status, schedules_df, sched_choice]
    )

    # Log Viewer
    log_files_dropdown.change(view_log_file, log_files_dropdown, log_content_display)
    refresh_logs_button.click(
        lambda: (gr.Dropdown(choices=get_log_files()), gr.Textbox(value="")),
        [],
        [log_files_dropdown, log_content_display]
    )
    demo.load(view_log_file, log_files_dropdown, log_content_display)

# --- Startup and Shutdown ---
sync_scheduler_from_db()
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

if __name__ == "__main__":
    demo.launch()