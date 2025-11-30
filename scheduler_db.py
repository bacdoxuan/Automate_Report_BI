import sqlite3
from typing import List, Dict, Any, Optional
from datetime import datetime

DB_FILE = "schedules.db"

def init_db():
    """Khởi tạo cơ sở dữ liệu và các bảng nếu chưa tồn tại."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        # Bảng lưu các lịch chạy
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name TEXT NOT NULL UNIQUE,
                frequency TEXT NOT NULL,
                day_of_week TEXT,
                run_time TEXT NOT NULL,
                skip_email BOOLEAN NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT 1
            )
        """)
        
        # Migration: Kiểm tra xem cột script_path đã tồn tại chưa, nếu chưa thì thêm vào
        cursor.execute("PRAGMA table_info(schedules)")
        columns = [info[1] for info in cursor.fetchall()]
        if "script_path" not in columns:
            cursor.execute("ALTER TABLE schedules ADD COLUMN script_path TEXT DEFAULT 'script.py'")

        # Bảng mới để lưu log các lần chạy
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schedule_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                run_timestamp TEXT NOT NULL,
                status TEXT NOT NULL, -- 'OK' hoặc 'NOK'
                details TEXT,
                FOREIGN KEY (schedule_id) REFERENCES schedules (id) ON DELETE CASCADE
            )
        """)
        conn.commit()

def log_run(schedule_id: int, status: str, details: str):
    """Ghi lại kết quả một lần chạy của lịch vào CSDL."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO schedule_runs (schedule_id, run_timestamp, status, details)
            VALUES (?, ?, ?, ?)
            """,
            (schedule_id, ts, status, details)
        )
        conn.commit()

def get_run_history(schedule_id: int) -> List[Dict[str, Any]]:
    """Lấy lịch sử chạy của một lịch cụ thể."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT run_timestamp, status, details FROM schedule_runs WHERE schedule_id = ? ORDER BY run_timestamp DESC",
            (schedule_id,)
        )
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

def add_schedule(name: str, freq: str, day: Optional[str], time: str, skip: bool, active: bool, script_path: str = "script.py") -> int:
    """Thêm một lịch mới vào CSDL."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO schedules (job_name, frequency, day_of_week, run_time, skip_email, is_active, script_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (name, freq, day, time, skip, active, script_path)
        )
        conn.commit()
        return cursor.lastrowid

def delete_schedule(schedule_id: int):
    """Xóa một lịch và các log chạy liên quan khỏi CSDL bằng ID."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        # Bật hỗ trợ khóa ngoại để xóa theo tầng (ON DELETE CASCADE)
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
        conn.commit()

def update_schedule_status(schedule_id: int, is_active: bool):
    """Cập nhật trạng thái active của một lịch."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE schedules SET is_active = ? WHERE id = ?",
            (is_active, schedule_id)
        )
        conn.commit()

def get_all_schedules() -> List[Dict[str, Any]]:
    """Lấy tất cả các lịch đã lưu từ CSDL."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM schedules ORDER BY id")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

def get_schedule(schedule_id: int) -> Optional[Dict[str, Any]]:
    """Lấy một lịch cụ thể bằng ID."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM schedules WHERE id = ?", (schedule_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

# Khởi tạo CSDL khi module được import
init_db()
