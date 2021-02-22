from enum import Enum
from repository.sqlRepo import SqlRepo


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
        pass

    def get_pipelines(self):
        pass

