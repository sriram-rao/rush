from repository.sqlRepo import SqlRepo


class PipelineRepo(SqlRepo):

    __instance = None

    def get_instance(self):
        if PipelineRepo.__instance is None:
            PipelineRepo.__instance = PipelineRepo()
        return PipelineRepo.__instance

    def __init__(self):
        super().__init__()

    def get_pipelines(self):
        pass
