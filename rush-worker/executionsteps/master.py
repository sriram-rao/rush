from jobs.job import Job
from domain.pipelineManager import PipelineManager


class Master(Job):
    pipelineManager = None

    def __init__(self):
        super().__init__()
        Master.pipelineManager = PipelineManager().get_instance()

    def run(self):
        PipelineManager.refresh()
        Master.pipelineManager.ping()
        token = Master.pipelineManager.become_master()
        if not token:
            return
        Master.pipelineManager.run_master_tasks()
