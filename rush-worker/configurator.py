import logging
from threading import current_thread


def configure():
    current_thread().name = "Main"
    logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s: %(message)s',
            handlers=[logging.FileHandler("logs/debug.log"), logging.StreamHandler()]
        )
