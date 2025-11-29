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

def run_script_manual(skip_email):
    """Starts the script for manual execution and returns immediate UI feedback."""
    command = [sys.executable, "script.py"]
    if skip_email:
        command.append("--skip-email")
    try:
        subprocess.Popen(command)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = "Chỉ xử lý file" if skip_email else "Toàn bộ quy trình"
        return f"[{timestamp}] Đã bắt đầu chạy tác vụ. Chế độ: {mode}. Xem tab 'Xem Logs' để theo dõi chi tiết."
    except Exception as e:
        return f"Lỗi khi bắt đầu tác vụ: {e}"

def run_scheduled_job(schedule_id: int, skip_email: bool):
    """
    Runs the script for a scheduled job, waits for it to complete, and logs the outcome.
    This function is executed by the APScheduler in a background thread.
    """
    command = [sys.executable, "script.py"]
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
            details = f"Lỗi khi chạy script.py. Stderr: {process.stderr[-500:]}"
        
        scheduler_db.log_run(schedule_id, status, details)
    except Exception as e:
        scheduler_db.log_run(schedule_id, "NOK", f"Lỗi nghiêm trọng khi khởi chạy job: {e}")


# --- Scheduler and DB Interaction Logic ---
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
            run_scheduled_job,
            trigger=trigger,
            id=job_id,
            args=[schedule['id'], schedule['skip_email']],
            replace_existing=True
        )
    except Exception as e:
        print(f"Error adding job {job_id} to scheduler: {e}")

def remove_job_from_scheduler(schedule_id: int):
    job_id = f"db_job_{schedule_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

def sync_scheduler_from_db():
    print("Syncing scheduler from database...")
    scheduler.remove_all_jobs()
    schedules = scheduler_db.get_all_schedules()
    for s in schedules:
        if s['is_active']:
            add_job_to_scheduler(s)
    print(f"Scheduler synced. {len(scheduler.get_jobs())} jobs are active.")

def load_schedules_as_dataframe():
    schedules = scheduler_db.get_all_schedules()
    if not schedules:
        return pd.DataFrame(columns=["ID", "Tên Lịch", "Tần Suất", "Ngày/Giờ Chạy", "Bỏ qua Email", "Trạng Thái"])
    
    df_data = []
    for s in schedules:
        run_details = f"{s['day_of_week'] if s['day_of_week'] else ''} @ {s['run_time']}".strip()
        df_data.append({
            "ID": s['id'], "Tên Lịch": s['job_name'], "Tần Suất": s['frequency'],
            "Ngày/Giờ Chạy": run_details, "Bỏ qua Email": "Có" if s['skip_email'] else "Không",
            "Trạng Thái": "Hoạt động" if s['is_active'] else "Dừng"
        })
    return pd.DataFrame(df_data)

def get_schedule_choices():
    schedules = scheduler_db.get_all_schedules()
    return [f"{s['job_name']} (ID: {s['id']})" for s in schedules]

def handle_add_schedule(name, freq, day, time, skip, active):
    if not name or not time:
        return "Tên lịch và thời gian chạy không được để trống.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())
    try:
        day_map = {"Thứ 2": "mon", "Thứ 3": "tue", "Thứ 4": "wed", "Thứ 5": "thu", "Thứ 6": "fri", "Thứ 7": "sat", "Chủ nhật": "sun"}
        day_str = day_map.get(day) if freq == "Hàng tuần" else None
        
        schedule_id = scheduler_db.add_schedule(name, freq, day_str, time, skip, active)
        if active:
            schedule = scheduler_db.get_schedule(schedule_id)
            if schedule: add_job_to_scheduler(schedule)
        
        return f"Đã thêm lịch '{name}'.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())
    except sqlite3.IntegrityError:
        return f"Lỗi: Tên lịch '{name}' đã tồn tại.", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())
    except Exception as e:
        return f"Lỗi: {e}", load_schedules_as_dataframe(), gr.Dropdown(choices=get_schedule_choices())

# --- Delete Confirmation Handlers ---
def prompt_delete(schedule_choice: str) -> List[Any]:
    """Shows the delete confirmation UI."""
    if not schedule_choice:
        return [gr.update(), gr.update(), gr.update(value="Vui lòng chọn một lịch để xóa.")]
    
    return [
        gr.update(visible=False), # Hide management buttons
        gr.update(visible=True),  # Show confirmation buttons
        gr.update(value=f"Bạn có chắc chắn muốn xóa lịch '{schedule_choice}' không? Hành động này không thể hoàn tác.")
    ]

def cancel_delete() -> List[Any]:
    """Hides the delete confirmation UI."""
    return [
        gr.update(visible=True),  # Show management buttons
        gr.update(visible=False), # Hide confirmation buttons
        gr.update(value="")
    ]

def handle_delete_schedule(schedule_choice: str) -> Tuple[Any, ...]:
    """Performs the deletion and resets the UI."""
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
            with gr.Tabs():
                with gr.TabItem("Thêm mới lịch chạy"):
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
                    
                with gr.TabItem("Quản lý lịch chạy đã có"):
                    gr.Markdown("### Danh sách và quản lý lịch chạy")
                    schedules_df = gr.DataFrame(load_schedules_as_dataframe, wrap=True, label="Danh sách lịch chạy")
                    gr.Markdown("### Quản lý")
                    sched_choice = gr.Dropdown(choices=get_schedule_choices(), label="Chọn lịch để quản lý")
                    
                    with gr.Group() as manage_buttons_group:
                        with gr.Row():
                            activate_button = gr.Button("✅ Kích hoạt")
                            deactivate_button = gr.Button("⛔ Dừng")
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

    # --- Event Handlers ---
    run_full_button.click(lambda: run_script_manual(False), [], manual_run_status)
    run_skip_email_button.click(lambda: run_script_manual(True), [], manual_run_status)
    
    add_button.click(fn=handle_add_schedule, inputs=[add_name, add_freq, add_dow, add_time, add_skip, add_active], outputs=[manage_status, schedules_df, sched_choice])

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
    demo.load(view_log_file, log_files_dropdown, log_content_display)

# --- Startup and Shutdown ---
sync_scheduler_from_db()
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

if __name__ == "__main__":
    demo.launch()