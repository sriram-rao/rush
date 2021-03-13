from threading import current_thread

from domain.pipelineManager import PipelineManager
from jobs.job import Job


class CompleteJob(Job):

    def run(self):
        manager = PipelineManager.get_instance()
        run = current_thread().assigned_run
        if run is None:
            return
        manager.update_run_status(run)
        manager.free_worker(current_thread().worker)
