import datetime

from domain.pipelineManager import PipelineManager
import importlib

from domain.runState import Status
from jobs.job import Job
from threading import current_thread


class RunJob(Job):
    pipelineManager = None

    def __init__(self):
        super().__init__()
        RunJob.pipelineManager = PipelineManager.get_instance()

    def run(self):
        # Run the job I was allocated and kept in context
        run = current_thread().assigned_run
        if run is None:
            return

        job_definition = RunJob.pipelineManager.get_job_definition(run.pipeline, run.job)
        full_class_name = job_definition.class_name
        module_name = full_class_name.rsplit('.', 1)[0]
        class_name = full_class_name.rsplit('.', 1)[1]
        job_class = getattr(importlib.import_module(module_name), class_name)
        params = {}
        job = job_class()
        try:
            job.initialise(params)
            job.run()
            run.status = Status.COMPLETE.value
        except Exception as e:
            self.logger.error(str(e))
            run.status = Status.FAILED.value
        finally:
            run.end_time = datetime.datetime.utcnow()
            current_thread().assigned_run = run
