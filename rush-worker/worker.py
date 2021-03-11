import logging
from threading import current_thread

from executionsteps.completeJob import CompleteJob
from executionsteps.getAllocatedJob import GetAllocatedJob
from executionsteps.leader import Leader
from executionsteps.runJob import RunJob

steps = [Leader(), GetAllocatedJob(), RunJob(), CompleteJob()]
logger = logging.getLogger(current_thread().name)


def work():
    logger.info("They call me the working man")
    for step in steps:
        step.run()
    logger.info("That's what I am")
