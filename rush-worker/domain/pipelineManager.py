import threading
import logging
from repository.pipelineRepo import PipelineRepo


class PipelineManager:
    repository = PipelineRepo()
    __instance = None

    @staticmethod
    def get_instance(self):
        if PipelineManager.__instance is None:
            PipelineManager.__instance = PipelineManager()
        return PipelineManager.__instance

    def __init__(self):
        context = threading.current_thread().__dict__
        logger = logging.getLogger(context["name"])
        if PipelineManager.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            PipelineManager.__instance = self
