import datetime
from enum import Enum
import os

from typing import List

from domain.runState import RunState, Status, InitiateRequest
from repository.sqlRepo import SqlRepo
from domain.pipeline import JobDefinition, Pipeline

INTERVAL = '2m'
UNIX_EPOCH = '1970-01-01'


class PipelineRepo(SqlRepo):

    __instance = None

    @staticmethod
    def get_instance():
        if PipelineRepo.__instance is None:
            PipelineRepo.__instance = PipelineRepo()
        return PipelineRepo.__instance

    def __init__(self):
        super().__init__()
        if PipelineRepo.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            PipelineRepo.__instance = self

    def get_master_token(self) -> bool:
        command = f"UPDATE master_token SET master = '{os.uname()[1]}', endtime = NOW() AT TIME ZONE 'utc' " \
                  f"+ INTERVAL '{INTERVAL}' WHERE endtime < NOW() AT TIME ZONE 'utc';"
        return self.execute(command) >= 1

    def get_pipelines(self):
        command = "SELECT name, definition FROM pipeline;"
        rows = self.fetch_entities(command)
        return [Pipeline(row) for row in rows]

    def process_waiting_jobs(self):
        command = f"UPDATE run_state SET status = '{Status.READY.value}' WHERE " \
                  f"status = '{Status.WAITING.value}' AND readytime < NOW() AT TIME ZONE 'utc';"
        self.execute(command)

    def get_runs_by_status(self, status: Status) -> [RunState]:
        query = f"SELECT * FROM run_state WHERE status = '{status.value}';"
        rows = self.fetch_entities(query)
        return [RunState().from_tuple(row) for row in rows]

    def archive(self, run_states: [RunState]) -> bool:
        if len(run_states) == 0:
            return True

        query = f"INSERT INTO archive(id, pipeline, job, jobinstance, status, worker, starttime, endtime, retry, readytime) VALUES "
        query += ",".join(f"({state.id}, '{state.pipeline}', '{state.job}', '{state.job_instance}', '{state.status}', "
                          f"'{state.worker}', '{self.get_default_if_none(state.start_time)}', "
                          f"'{self.get_default_if_none(state.end_time)}', {state.retry}, "
                          f"'{self.get_default_if_none(state.ready_time)}')"
                          for state in run_states)
        query += ';'
        return self.execute(query) == len(run_states)

    def remove_runs(self, run_states: [RunState]) -> bool:
        if len(run_states) == 0:
            return True
        command = f"DELETE FROM run_state WHERE id IN ({','.join(str(r.id) for r in run_states)});"
        return self.execute(command) == len(run_states)

    def update_statuses(self, statuses: [RunState]):
        if len(statuses) == 0:
            return True
        command = ";".join(f"UPDATE run_state SET worker = '{s.worker}', status = '{s.status}', endtime = "
                           f"'{self.get_default_if_none(s.end_time)}' WHERE id = {s.id}" for s in statuses)
        return self.execute(command) == len(statuses)

    def insert_runs(self, run_states: [RunState]) -> bool:
        if len(run_states) == 0:
            return True
        query = f"INSERT INTO run_state(pipeline, job, jobinstance, status, worker, starttime, endtime, retry, readytime) VALUES "
        query += ",".join(f"('{state.pipeline}', '{state.job}', '{state.job_instance}', '{state.status}', "
                          f"'{state.worker}', '{self.get_default_if_none(state.start_time)}', "
                          f"'{self.get_default_if_none(state.end_time)}', {state.retry}, "
                          f"'{self.get_default_if_none(state.ready_time)}')"
                          for state in run_states)
        query += ';'
        return self.execute(query) == len(run_states)

    def get_jobs_to_initiate(self) -> [InitiateRequest]:
        query = "SELECT id, pipeline, job, jobinstance, status FROM initiate_request WHERE status = 'Pending';"
        rows = self.fetch_entities(query)
        return [InitiateRequest(row) for row in rows]

    def complete_initiation(self, requests: [InitiateRequest]) -> bool :
        if len(requests) == 0:
            return True
        command = f"UPDATE initiate_request SET status = 'Complete' WHERE id IN ({','.join(str(r.id) for r in requests)});"
        return self.execute(command) == len(requests)

    def update_last_seen(self, worker: str) -> bool:
        command = f"UPDATE worker SET lastseen = NOW() AT TIME ZONE 'utc' WHERE name = '{worker}'"
        return self.execute(command) == 1

    def worker_exists(self, worker: str) -> bool:
        query = f"SELECT COUNT(*) FROM worker WHERE name = '{worker}'"
        return self.fetch_entity(query)[0] == 1

    def add_worker(self, worker: str) -> bool:
        command = f"INSERT INTO worker (name, lastseen, status, run_id) VALUES('{worker}', NOW() AT TIME ZONE 'utc', 'Free', NULL)"
        return self.execute(command) == 1

    def get_workers_by_status(self, status: str):
        query = f"SELECT * FROM worker WHERE status = '{status}'"
        return None

    def get_default_if_none(self, time: datetime) -> str:
        return str(time) if time is not None else UNIX_EPOCH
