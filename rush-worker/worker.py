import logging
from threading import current_thread

from executionsteps.complete_job import CompleteJob
from executionsteps.get_allocated_job import GetAllocatedJob
from executionsteps.master import Master
from executionsteps.run_job import RunJob

steps = [Master(), GetAllocatedJob(), RunJob(), CompleteJob()]
logger = logging.getLogger(current_thread().name)


def work():

    logger.info("They call me the working man")
    for step in steps:
        step.run()
    logger.info("That's what I am")
