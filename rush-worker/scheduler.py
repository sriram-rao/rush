import logging
import threading
import time

import worker


class Scheduler:
    def start(self):
        pass


class PeriodicScheduler(Scheduler):
    def __init__(self):
        context = threading.current_thread().__dict__
        self.logger = logging.getLogger(context["name"])

    def start(self):
        while True:
            self.logger.info("Starting scheduler")
            worker.work()
            time.sleep(5)
