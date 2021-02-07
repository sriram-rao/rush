import logging
import threading
import time


class Scheduler:
    def start(self):
        pass


class PeriodicScheduler(Scheduler):
    def __init__(self):
        context = threading.local()
        self.logger = logging.getLogger(context.name)

    def start(self):
        while True:
            self.logger.log(logging.INFO, "Starting scheduler")
            # run tasks
            time.sleep(10)
