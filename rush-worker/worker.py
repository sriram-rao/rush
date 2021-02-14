import logging
import threading


steps = {}

context = threading.current_thread().__dict__
logger = logging.getLogger(context["name"])


def work():
    logger.info("They call me the working man")
    for step in steps:
        step.run()
    logger.info("That's what I am")
