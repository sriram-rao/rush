import os

from jobs.job import Job


class RunCommand(Job):
    def run(self):
        command = self.params.get("command_to_run")
        os.system(command)
