import logging
import time
from threading import current_thread

import worker


class Scheduler:
    def start(self):
        pass


class PeriodicScheduler(Scheduler):
    def __init__(self):
        self.logger = logging.getLogger(current_thread().name)

    def start(self):
        while True:
            worker.work()
            time.sleep(5)
