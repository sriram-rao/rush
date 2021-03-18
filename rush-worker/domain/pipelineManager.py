import datetime
import logging
import os
from threading import current_thread

from domain.pipeline import JobDefinition
from domain.runState import RunState, Status, Worker
from repository.pipelineRepo import PipelineRepo


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
        self.logger = logging.getLogger(current_thread().name)
        if PipelineManager.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            PipelineManager.__instance = self

    def ping(self):
        worker = current_thread().worker
        if not PipelineManager.repository.worker_exists(worker):
            PipelineManager.repository.add_worker(worker)
        PipelineManager.repository.update_last_seen(worker)

    def become_master(self) -> bool:
        return PipelineManager.repository.get_master_token()

    def run_master_tasks(self) -> None:
        # add scheduled jobs to trigger job1 -> job2
        try:
            PipelineManager.repository.begin_transaction()
            self.process_complete_jobs()
            self.process_timed_out_jobs()
            self.process_failed_jobs()
            self.process_run_requests()
            self.process_waiting_jobs()
            self.assign_jobs_to_workers()
            PipelineManager.repository.commit_transaction()
        except Exception as e:
            self.logger.error(str(e))
            PipelineManager.repository.rollback_transaction()

    def process_waiting_jobs(self):
        # for all waiting jobs if the ready time < current time, update status to ready
        PipelineManager.repository.process_waiting_jobs()

    def process_complete_jobs(self):
        # for all complete jobs, move them to archive and (can be done later: ) trigger child jobs
        complete_jobs = PipelineManager.repository.get_runs_by_status(Status.COMPLETE)
        new_runs = []
        for run in complete_jobs:
            pipeline = self.get_pipeline(run.pipeline)
            job = self.get_job_definition(run.pipeline, run.job)
            index = pipeline.jobs.index(job)
            if index == len(pipeline.jobs) - 1:
                continue
            next_job = pipeline.jobs[index + 1]
            new_runs.append(RunState().get_new_run(next_job))
        if not PipelineManager.repository.archive(complete_jobs):
            raise Exception('Archive failed!')
        if not PipelineManager.repository.remove_runs(complete_jobs):
            raise Exception('Remove runs failed!')
        if not PipelineManager.repository.insert_runs(new_runs):
            raise Exception('New runs failed')

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

    def process_run_requests(self):
        # check all pending trigger requests and add them as ready jobs in running state
        requests = PipelineManager.repository.get_run_requests()
        new_runs = [request.get_run_state() for request in requests]
        if not PipelineManager.repository.insert_runs(new_runs) \
                or not PipelineManager.repository.commit_run_request(requests):
            raise Exception("Error in run requests")

    def assign_jobs_to_workers(self):
        # get the list of ready jobs and the list of available workers and assign one-to-one
        ready_jobs = PipelineManager.repository.get_runs_by_status(Status.READY)
        free_workers = PipelineManager.repository.get_workers_by_status('Free')
        assigned_states = []
        assigned_workers = []
        end_assignment = min(len(ready_jobs), len(free_workers))
        for i in range(0, end_assignment):
            assigned_states.append(ready_jobs[i].assign(free_workers[i]))
            assigned_workers.append(free_workers[i].assign(ready_jobs[i]))
        PipelineManager.repository.update_statuses(assigned_states)
        PipelineManager.repository.update_worker_statuses(assigned_workers)

    def get_pipeline(self, pipeline: str):
        return PipelineManager.pipelines[pipeline]

    def get_job_definition(self, pipeline: str, job: str) -> JobDefinition:
        pipeline_definition = PipelineManager.pipelines[pipeline]
        return next(j for j in pipeline_definition.jobs if j.name == job)

    def get_assigned_job(self, worker_name: str) -> RunState:
        worker = PipelineManager.repository.get_worker(worker_name)
        run = PipelineManager.repository.get_assigned_run(worker_name)
        if run is None or worker.run_id != run.id:
            if run is not None:
                run.handle_worker_failure()
                PipelineManager.repository.update_statuses([run])
            worker.status = 'Free'
            PipelineManager.repository.update_worker_statuses([worker])
        return run

    def update_run_status(self, run: RunState):
        if run is None:
            return
        if not PipelineManager.repository.update_statuses([run]):
            raise Exception("Couldn't update status")

    def free_worker(self, worker_name: str):
        worker = Worker()
        worker.name = worker_name
        if not PipelineManager.repository.update_worker_statuses([worker]):
            raise Exception("Couldn't free worker")

    def get_parameters(self, pipeline: str, job: str) -> dict:
        rows = PipelineManager.repository.get_parameters(pipeline, job)
        return {row[0]: row[1] for row in rows}

    @staticmethod
    def refresh():
        PipelineManager.pipelines = {pipeline.name: pipeline
                                     for pipeline in PipelineManager.repository.get_pipelines()}
        # all_pipelines = PipelineManager.repository.get_pipelines()
        # for pipeline in all_pipelines:
        #     PipelineManager.pipelines[pipeline.name] = pipeline
