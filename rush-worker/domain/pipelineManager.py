from threading import current_thread
import logging
from repository.pipelineRepo import PipelineRepo


class PipelineManager:
    repository = PipelineRepo()
    __instance = None

    @staticmethod
    def get_instance():
        if PipelineManager.__instance is None:
            PipelineManager.__instance = PipelineManager()
        return PipelineManager.__instance

    def __init__(self):
        # logger = logging.getLogger(current_thread().name)
        if PipelineManager.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            PipelineManager.__instance = self

    def become_leader(self) -> bool:
        return PipelineManager.repository.get_leader_baton()

    def run_leader_tasks(self) -> None:
        # add scheduled jobs to trigger
        self.process_waiting_jobs()
        self.process_complete_jobs()
        self.process_timed_out_jobs()
        self.process_failed_jobs()
        self.trigger_jobs()
        self.assign_jobs_to_workers()

    def process_waiting_jobs(self):
        # for all waiting jobs if the ready time < current time, update status to ready
        pass

    def process_complete_jobs(self):
        # for all complete jobs, move them to archive and (can be done later: ) trigger child jobs
        pass

    def process_timed_out_jobs(self):
        # for all running jobs, check if they are timed out. If they are, mark them failed
        pass

    def process_failed_jobs(self):
        # for all failed jobs, add them in archive and
        # add a waiting entry to be ready after the configured retry interval
        pass

    def trigger_jobs(self):
        # check all pending trigger requests and add them as ready jobs in running state
        pass

    def assign_jobs_to_workers(self):
        # get the list of ready jobs and the list of available workers and assign one-to-one
        pass
