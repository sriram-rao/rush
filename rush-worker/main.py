import configurator
from scheduler import PeriodicScheduler

if __name__ == '__main__':
    # Set up worker
    configurator.configure()
    # Call scheduler
    scheduler = PeriodicScheduler()
    scheduler.start()
