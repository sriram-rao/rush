from domain.pipelineManager import PipelineManager
from jobs.job import Job


class Master(Job):
    pipelineManager = None

    def __init__(self):
        super().__init__()
        Master.pipelineManager = PipelineManager.get_instance()

    def run(self):
        PipelineManager.refresh()
        token = Master.pipelineManager.become_master()
        if not token:
            return
        Master.pipelineManager.run_master_tasks()
