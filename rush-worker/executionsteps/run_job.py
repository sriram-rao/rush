import datetime
import importlib
from threading import current_thread

from domain.state import Status
from executionsteps.step import Step


class RunJob(Step):
    def run(self):
        # Run the job I was allocated and kept in context
        run = current_thread().assigned_run
        if run is None:
            return

        job_definition = RunJob.pipeline_manager.get_job_definition(run.pipeline, run.job)
        params = RunJob.pipeline_manager.get_parameters(run.pipeline, run.job)
        full_class_name = job_definition.class_name
        module_name = full_class_name.rsplit('.', 1)[0]
        class_name = full_class_name.rsplit('.', 1)[1]
        try:
            job_class = getattr(importlib.import_module(module_name), class_name)
            job = job_class()
            job.initialise(params)
            job.run()
            run.status = Status.COMPLETE.value
        except Exception as e:
            self.logger.error(str(e))
            run.status = Status.FAILED.value
        finally:
            run.end_time = datetime.datetime.utcnow()
            current_thread().assigned_run = run
