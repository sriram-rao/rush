import os

from jobs.job import Job


class RunCommand(Job):
    def run(self):
        command = self.params.get("command_to_run")
        output = self.params.get("piped_output_command") 
        os.system(command + output)
