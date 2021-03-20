from threading import current_thread

from executionsteps.step import Step


class GetAllocatedJob(Step):
    def run(self):
        # Check if I have any jobs allocated to run
        GetAllocatedJob.pipeline_manager.ping()
        worker = current_thread().worker
        run = GetAllocatedJob.pipeline_manager.get_assigned_job(worker)
        current_thread().assigned_run = run
