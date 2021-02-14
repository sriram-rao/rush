import logging
import threading


class Job:
    def __init__(self, settings: dict):
        context = threading.current_thread().__dict__
        self.logger = logging.getLogger(context["name"])

    def run(self):
        pass
