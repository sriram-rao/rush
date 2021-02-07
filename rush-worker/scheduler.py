import time


class Scheduler:
    def start(self):
        pass


class PeriodicScheduler(Scheduler):
    def start(self):
        while True:
            # run tasks
            time.sleep(10)
