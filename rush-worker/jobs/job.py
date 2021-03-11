import logging
from threading import current_thread


class Job:
    def __init__(self):
        self.logger = logging.getLogger(current_thread().name)

    def run(self):
        pass
