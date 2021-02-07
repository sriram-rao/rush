import logging
import threading


def configure():
    context = threading.current_thread().__dict__
    context["name"] = "Main"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        handlers=[logging.FileHandler("logs/debug.log"), logging.StreamHandler()]
        )
