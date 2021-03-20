from domain.pipeline_manager import PipelineManager
from executionsteps.step import Step


class Master(Step):
    def run(self):
        PipelineManager.refresh()
        token = Master.pipeline_manager.become_master()
        if not token:
            return
        Master.pipeline_manager.run_master_tasks()
