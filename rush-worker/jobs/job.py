import logging
from threading import current_thread


class Job:
    def __init__(self):
        self.logger = logging.getLogger(current_thread().name)
        self.params = {}

    def initialise(self, params: dict = None):
        self.params = params

    def run(self):
        pass
