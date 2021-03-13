from domain.pipelineManager import PipelineManager
from domain.runState import RunState
from jobs.job import Job
from threading import current_thread


class GetAllocatedJob(Job):
    pipelineManager = None

    def __init__(self):
        super().__init__()
        GetAllocatedJob.pipelineManager = PipelineManager.get_instance()

    def run(self):
        # Check if I have any jobs allocated to run
        GetAllocatedJob.pipelineManager.ping()
        worker = current_thread().worker
        run = GetAllocatedJob.pipelineManager.get_assigned_job(worker)
        current_thread().assigned_run = run
