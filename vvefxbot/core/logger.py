import logging
import os
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv

def get_logger(module_name: str) -> logging.Logger:
    """
    Creates and returns a logger instance with file and console handlers.
    
    Args:
        module_name (str): The name of the module for which the logger is created.
        
    Returns:
        logging.Logger: The configured logger instance.
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    # Prevent adding multiple handlers if the logger already exists
    if not logger.handlers:
        # Create logs directory if it doesn't exist
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_format = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s')

        # File Handler (Daily rotation, keep 30 days)
        file_handler = TimedRotatingFileHandler(
            filename=os.path.join(log_dir, "bot.log"),
            when="midnight",
            interval=1,
            backupCount=30
        )
        file_handler.setFormatter(log_format)

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)

        # Sensitive Information Masking Filter
        class SensitiveFilter(logging.Filter):
            """Filter to mask sensitive environment variable values in logs."""
            def __init__(self):
                super().__init__()
                load_dotenv()
                self.sensitive_keys = [
                    "MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER",
                    "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID",
                    "GOOGLE_SHEET_ID", "GOOGLE_CREDS_PATH"
                ]
                self.sensitive_values = [os.getenv(k) for k in self.sensitive_keys if os.getenv(k)]

            def filter(self, record):
                if not isinstance(record.msg, str):
                    return True
                for val in self.sensitive_values:
                    if val and val in record.msg:
                        record.msg = record.msg.replace(val, "********")
                return True

        # Add filter to both handlers
        sensitive_filter = SensitiveFilter()
        file_handler.addFilter(sensitive_filter)
        console_handler.addFilter(sensitive_filter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
