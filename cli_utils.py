"""Tiện ích CLI dùng chung cho script.py và downloademailonly.py.

Tách riêng để ``downloademailonly.py`` không phải ``import script``
(tránh kéo theo pandas / numpy / openpyxl / 4 processor chỉ để parse ngày).

Không import bất kỳ module nặng nào ở đây.
"""

import argparse
from datetime import datetime

DATE_FORMAT = "%Y-%m-%d"


def build_arg_parser(description: str = "Automated BI Report Generator.") -> argparse.ArgumentParser:
    """Tạo argparse parser dùng chung (script.py / downloademailonly.py)."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-s", "--skip-email",
        action="store_true",
        help="Skip email connection and download steps, process local files directly."
    )
    parser.add_argument(
        "-d", "--process-date",
        type=str,
        help="Date(s) to process (YYYY-MM-DD). Supports a comma-separated list for "
             "backfill, e.g. '2026-10-01,2026-10-02,2026-10-03'. "
             "Defaults to yesterday if not provided."
    )
    return parser


def parse_date_list(date_str: str) -> list:
    """Parse chuỗi ngày cách nhau bằng dấu ',' (YYYY-MM-DD) thành list datetime.

    - Lặp lại (duplicate) được gộp về một.
    - Trật tự input được giữ nguyên (không sort) để chạy đúng thứ tự user chọn.
    - Token phải đúng 10 ký tự YYYY-MM-DD (strptime vốn chấp nhận '2026-1-5').
    - Nếu có token sai định dạng: raise ValueError liệt kê các token lỗi.
    """
    if not date_str or not date_str.strip():
        return []
    dates = []
    seen = set()
    invalid = []
    for token in date_str.split(','):
        token = token.strip()
        if not token:
            continue
        try:
            dt = datetime.strptime(token, DATE_FORMAT)
        except ValueError:
            invalid.append(token)
            continue
        if len(token) != 10:
            invalid.append(token)
            continue
        if dt.date() in seen:
            continue
        seen.add(dt.date())
        dates.append(dt)
    if invalid:
        raise ValueError(
            f"Ngày không hợp lệ: {', '.join(invalid)}. "
            f"Vui lòng dùng định dạng YYYY-MM-DD."
        )
    return dates
