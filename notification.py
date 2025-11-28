
import os
from datetime import datetime
from exchange_lib import send_email

class Notifier:
    def __init__(self, account, recipients, subject_prefix="[Auto Report]"):
        self.account = account
        self.recipients = recipients
        self.subject_prefix = subject_prefix
        
    def send_success(self, log_file=None):
        """Gửi email thông báo thành công"""
        subject = f"{self.subject_prefix} ✅ Data Processing Completed Successfully - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
✅ Data Processing Completed Successfully
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

All steps in the pipeline have been executed without errors.
Please check the attached log file for details.

This is an automated message.
        """
        
        attachments = [log_file] if log_file else []
        return send_email(self.account, self.recipients, subject, body, attachments)
        
    def send_failure(self, step_name, error_msg, log_file=None):
        """Gửi email thông báo lỗi"""
        subject = f"{self.subject_prefix} ❌ Data Processing FAILED at {step_name} - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
❌ Data Processing Failed
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Failed Step: {step_name}

Error Message:
{error_msg}

Please check the attached log file for full traceback.

This is an automated message.
        """
        
        attachments = [log_file] if log_file else []
        return send_email(self.account, self.recipients, subject, body, attachments)
