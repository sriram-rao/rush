import datetime
import os
from threading import current_thread
import logging
from repository.pipelineRepo import PipelineRepo
from domain.pipeline import JobDefinition
from domain.runState import RunState, Status


class PipelineManager:
    repository = PipelineRepo().get_instance()
    __instance = None
    pipelines = {}

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

    def ping(self):
        worker = os.uname()[1]
        if not PipelineManager.repository.worker_exists(worker):
            PipelineManager.repository.add_worker(worker)
        PipelineManager.repository.update_last_seen(worker)

    def become_master(self) -> bool:
        return PipelineManager.repository.get_master_token()

    def run_master_tasks(self) -> None:
        # add scheduled jobs to trigger job1 -> job2
        self.process_complete_jobs()
        self.process_timed_out_jobs()
        self.process_failed_jobs()
        self.initiate_jobs()
        self.process_waiting_jobs()
        self.assign_jobs_to_workers()

    def process_waiting_jobs(self):
        # for all waiting jobs if the ready time < current time, update status to ready
        PipelineManager.repository.process_waiting_jobs()

    def process_complete_jobs(self):
        # for all complete jobs, move them to archive and (can be done later: ) trigger child jobs
        complete_jobs = PipelineManager.repository.get_runs_by_status(Status.COMPLETE)
        if not PipelineManager.repository.archive(complete_jobs):
            raise Exception('Archive failed!')
        if not PipelineManager.repository.remove_runs(complete_jobs):
            raise Exception('Remove runs failed!')

    def process_timed_out_jobs(self):
        # for all running jobs, check if they are timed out. If they are, mark them failed
        running_jobs = PipelineManager.repository.get_runs_by_status(Status.RUNNING)
        timed_out_jobs = []
        for running_job in running_jobs:
            job = self.get_job_definition(running_job.pipeline, running_job.job)
            if datetime.datetime.utcnow() < running_job.start_time + datetime.timedelta(minutes=job.timeout_minutes):
                continue
            running_job.timeout()
            timed_out_jobs.append(running_job)
        if not PipelineManager.repository.update_statuses(timed_out_jobs):
            raise Exception("Timed out jobs failed")

    def process_failed_jobs(self):
        # for all failed jobs, add them in archive and
        # add a waiting entry to be ready after the configured retry interval
        failed_jobs = PipelineManager.repository.get_runs_by_status(Status.FAILED)
        new_runs = []
        for failed_job in failed_jobs:
            job_def = self.get_job_definition(failed_job.pipeline, failed_job.job)
            new_runs.append(failed_job.get_retry(job_def))
        if not PipelineManager.repository.remove_runs(failed_jobs) \
                or not PipelineManager.repository.archive(failed_jobs):
            raise Exception("Archive runs failed")
        if not PipelineManager.repository.insert_runs(new_runs):
            raise Exception("Insert runs failed")

    def initiate_jobs(self):
        # check all pending trigger requests and add them as ready jobs in running state
        requests = PipelineManager.repository.get_jobs_to_initiate()
        new_runs = [request.initiate() for request in requests]
        if not PipelineManager.repository.insert_runs(new_runs) \
                or not PipelineManager.repository.complete_initiation(requests):
            raise Exception("Initiation crisis")

    def assign_jobs_to_workers(self):
        # get the list of ready jobs and the list of available workers and assign one-to-one
        ready_jobs = PipelineManager.repository.get_runs_by_status(Status.READY)
        free_workers = PipelineManager.repository.get_workers_by_status('Free')
        assigned_states = []
        assigned_workers = []
        for i in range(0, len(free_workers)):
            assigned_states.append(ready_jobs[i].assign[free_workers[i]])
            assigned_workers.append(free_workers[i])
        PipelineManager.repository.update_statuses(assigned_states)
        PipelineManager.repository.update_worker_statuses(assigned_workers)

    def get_job_definition(self, pipeline: str, job: str) -> JobDefinition:
        pipeline_definition = PipelineManager.pipelines[pipeline]
        return next(j for j in pipeline_definition.jobs if j.name == job)

    @staticmethod
    def refresh():
        PipelineManager.pipelines = {pipeline.name: pipeline
                                     for pipeline in PipelineManager.repository.get_pipelines()}
        # all_pipelines = PipelineManager.repository.get_pipelines()
        # for pipeline in all_pipelines:
        #     PipelineManager.pipelines[pipeline.name] = pipelinez`
