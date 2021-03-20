from threading import current_thread

from executionsteps.step import Step


class CompleteJob(Step):

    def run(self):
        run = current_thread().assigned_run
        if run is None:
            return
        Step.pipeline_manager.update_run_status(run)
        Step.pipeline_manager.free_worker(current_thread().worker)
