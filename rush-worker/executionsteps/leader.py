from jobs.job import Job
from domain.pipelineManager import PipelineManager


class Leader(Job):
    pipelineManager = None

    def __init__(self):
        super().__init__()
        Leader.pipelineManager = PipelineManager().get_instance()

    def run(self):
        leader = Leader.pipelineManager.become_leader()
        if not leader:
            return
        Leader.pipelineManager.run_leader_tasks()
