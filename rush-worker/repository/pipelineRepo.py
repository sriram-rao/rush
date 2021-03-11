from enum import Enum
import os
from repository.sqlRepo import SqlRepo

INTERVAL = '2m'


class Status(Enum):
    WAITING = "Waiting",
    READY = "Ready",
    RUNNING = "Running",
    COMPLETE = "Complete",
    FAILED = "Failed",
    STUCK = "Stuck"


class PipelineRepo(SqlRepo):

    __instance = None

    def get_instance(self):
        if PipelineRepo.__instance is None:
            PipelineRepo.__instance = PipelineRepo()
        return PipelineRepo.__instance

    def __init__(self):
        super().__init__()

    def get_leader_baton(self) -> bool:
        command = f"UPDATE leaderbaton SET leader = '{os.uname()[1]}', " \
                  f"endtime = NOW() + INTERVAL '{INTERVAL}' WHERE endtime < NOW();"
        return self.execute(command) >= 1

    def get_pipelines(self):
        pass

