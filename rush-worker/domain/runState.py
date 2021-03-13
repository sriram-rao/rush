import copy
import datetime
from enum import Enum

from domain.pipeline import JobDefinition


class Status(Enum):
    WAITING = "Waiting"
    READY = "Ready"
    RUNNING = "Running"
    COMPLETE = "Complete"
    FAILED = "Failed"
    STUCK = "Stuck"


class RunState:
    def __init__(self):
        self.id = 0
        self.pipeline = ""
        self.job = ""
        self.job_instance = 0
        self.status = "Ready"
        self.worker = ""
        self.start_time = None
        self.end_time = None
        self.retry = 0
        self.ready_time = datetime.datetime.utcnow()

    def from_tuple(self, row: tuple):
        if row is None:
            return None
        self.id = row[0]
        self.pipeline = row[1]
        self.job = row[2]
        self.job_instance = row[3]
        self.status = row[4]
        self.worker = row[5]
        self.start_time = row[6]
        self.end_time = row[7]
        self.retry = row[8]
        self.ready_time = row[9]  # TODO: change to use string keys
        return self

    def timeout(self):
        self.status = Status.FAILED.value
        self.end_time = datetime.datetime.utcnow()
        return self

    def assign(self, worker):
        self.status = Status.RUNNING.value
        self.worker = worker.name
        self.start_time = datetime.datetime.utcnow()
        return self

    def handle_worker_failure(self):
        self.status = Status.FAILED.value
        self.end_time = datetime.datetime.utcnow()

    def get_retry(self, definition: JobDefinition):
        state = copy.deepcopy(self)
        state.status = Status.WAITING.value
        state.retry = self.retry + 1
        state.ready_time = self.end_time + datetime.timedelta(minutes=definition.retry_after_minutes)
        state.start_time = None
        state.end_time = None
        state.worker = ''
        return state

    def get_new_run(self, job_definition: JobDefinition):
        new_run = RunState()
        new_run.pipeline = job_definition.pipeline
        new_run.job = job_definition.name
        return new_run


class RunRequest:
    def __init__(self, row: tuple):
        if row is None:
            return
        self.id = row[0]
        self.pipeline = row[1]
        self.job = row[2]
        self.job_instance = row[3]
        self.status = row[4]

    def get_run_state(self) -> RunState:
        state = RunState()
        state.pipeline = self.pipeline
        state.job = self.job
        state.job_instance = self.job_instance
        return state


class Worker:
    def __init__(self):
        self.name = ''
        self.last_seen = datetime.datetime.utcnow()
        self.status = 'Free'
        self.run_id = -1

    def from_tuple(self, row: tuple):
        if row is None:
            return None
        self.name = row[0]
        self.last_seen = row[1]
        self.status = row[2]
        self.run_id = row[3]
        return self

    def assign(self, run_state: RunState):
        self.run_id = run_state.id
        self.status = 'Busy'
        return self
