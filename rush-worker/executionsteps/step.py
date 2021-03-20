from domain.pipeline_manager import PipelineManager
from jobs.job import Job


class Step(Job):
    pipeline_manager = None

    def __init__(self):
        super().__init__()
        Step.pipeline_manager = PipelineManager.get_instance()
