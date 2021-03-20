import datetime
import os

from domain.pipeline import Pipeline
from domain.state import RunState, Status, RunRequest, Worker
from repository.sql_repo import SqlRepo

INTERVAL = '2m'


# TODO ADD CONFIG.ini functionality.

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
        query = f"SELECT id, pipeline, job, jobinstance, status, worker, starttime, endtime, retry, readytime, message " \
                f"FROM run_state WHERE status = '{status.value}';"
        rows = self.fetch_entities(query)
        return [RunState().from_tuple(row) for row in rows]

    def archive(self, run_states: [RunState]) -> bool:
        if len(run_states) == 0:
            return True

        query = f"INSERT INTO archive(id, pipeline, job, jobinstance, status, worker, starttime, endtime, retry, readytime) VALUES "
        query += ",".join(f"({state.id}, '{state.pipeline}', '{state.job}', '{state.job_instance}', '{state.status}', "
                          f"'{state.worker}', {self.get_default_if_none(state.start_time)}, "
                          f"{self.get_default_if_none(state.end_time)}, {state.retry}, "
                          f"{self.get_default_if_none(state.ready_time)})"
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
                           f"{self.get_default_if_none(s.end_time)}, "
                           f"starttime = {self.get_default_if_none(s.start_time)} WHERE id = {s.id}" for s in statuses)
        return self.execute(command) == len(statuses)

    def insert_runs(self, run_states: [RunState]) -> bool:
        if len(run_states) == 0:
            return True
        query = f"INSERT INTO run_state(pipeline, job, jobinstance, status, worker, starttime, endtime, retry, readytime) VALUES "
        query += ",".join(f"('{state.pipeline}', '{state.job}', '{state.job_instance}', '{state.status}', "
                          f"'{state.worker}', {self.get_default_if_none(state.start_time)}, "
                          f"{self.get_default_if_none(state.end_time)}, {state.retry}, "
                          f"{self.get_default_if_none(state.ready_time)})"
                          for state in run_states)
        query += ';'
        return self.execute(query) == len(run_states)

    def get_run_requests(self) -> [RunRequest]:
        query = "SELECT id, pipeline, job, jobinstance, status FROM run_request WHERE status = 'Pending';"
        rows = self.fetch_entities(query)
        return [RunRequest(row) for row in rows]

    def commit_run_request(self, requests: [RunRequest]) -> bool:
        if len(requests) == 0:
            return True
        command = f"UPDATE run_request SET status = 'Complete' WHERE id IN ({','.join(str(r.id) for r in requests)});"
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
        query = f"SELECT name, lastseen, status, run_id FROM worker WHERE status = '{status}'"
        rows = self.fetch_entities(query)
        return [Worker().from_tuple(worker) for worker in rows]

    def update_worker_statuses(self, workers: [Worker]):
        if len(workers) == 0:
            return True
        command = ";".join(f"UPDATE worker SET status = '{w.status}', run_id = {w.run_id if w.run_id is not None else 'NULL'} "
                           f"WHERE name = '{w.name}'" for w in workers)
        return self.execute(command) == len(workers)

    def get_worker(self, worker_name: str):
        query = f"SELECT name, lastseen, status, run_id FROM worker WHERE name = '{worker_name}';"
        return Worker().from_tuple(self.fetch_entity(query))

    def get_assigned_run(self, worker_name: str) -> RunState:
        query = f"SELECT id, pipeline, job, jobinstance, status, worker, starttime, endtime, retry, readytime, message " \
                f"FROM run_state WHERE worker = '{worker_name}' AND status = '{Status.RUNNING.value}';"
        return RunState().from_tuple(self.fetch_entity(query))

    def get_parameters(self, pipeline: str, job: str):
        query = f"SELECT key, value FROM job_parameter WHERE pipeline = '{pipeline}' AND job = '{job}'"
        return self.fetch_entities(query)

    def get_default_if_none(self, time: datetime) -> str:
        return f"'{str(time)}'" if time is not None else 'NULL'
