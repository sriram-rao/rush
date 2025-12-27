import logging
import os
from threading import current_thread


def configure(worker_name: str):
    current_thread().name = "Main"
    current_thread().worker = os.uname()[1] + '-' + worker_name
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)

    # Only configure logging once to avoid duplicate handlers if configure is called multiple times
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(f"logs/{worker_name}.log", encoding='utf-8'),
                logging.StreamHandler(),
            ],
        )
    else:
        # If already configured, ensure the level is at least INFO
        root_logger.setLevel(logging.INFO)
