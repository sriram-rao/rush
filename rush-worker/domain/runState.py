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

    def timeout(self):
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


class InitiateRequest:
    def __init__(self, row: tuple):
        self.id = row[0]
        self.pipeline = row[1]
        self.job = row[2]
        self.job_instance = row[3]
        self.status = row[4]

    def initiate(self) -> RunState:
        state = RunState()
        state.pipeline = self.pipeline
        state.job = self.job
        state.job_instance = self.job_instance
        return state
