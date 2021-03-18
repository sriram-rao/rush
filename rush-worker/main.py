import sys

import configurator
from scheduler import PeriodicScheduler

if __name__ == '__main__':
    # Set up worker
    worker_name = sys.argv[1] if len(sys.argv) > 1 else 'thread1'
    configurator.configure(worker_name)
    # Call scheduler
    scheduler = PeriodicScheduler()
    scheduler.start()
