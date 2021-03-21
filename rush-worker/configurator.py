import logging
import os
from threading import current_thread


def configure(worker_name: str):
    current_thread().name = "Main"
    current_thread().worker = os.uname()[1] + '-' + worker_name
    logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s: %(message)s',
            handlers=[logging.FileHandler(f"logs/{worker_name}.log"), logging.StreamHandler()]
        )
