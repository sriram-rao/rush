import subprocess

from jobs.job import Job


class RunCommand(Job):
    def run(self):
        # Retrieve command parameters
        command = self.params.get("command_to_run")
        pipe_cmd = self.params.get("piped_output_command")

        if not command or not isinstance(command, str):
            # Log and raise if command is missing or invalid
            self.logger.error("Missing or invalid 'command_to_run' parameter")
            raise Exception("Missing 'command_to_run' parameter")

        # Build the final command string, supporting optional piping
        full_cmd = command if not pipe_cmd else f"{command} | {pipe_cmd}"

        self.logger.info(f"Executing command: {full_cmd}")

        try:
            # Use subprocess to execute and capture outputs for logging
            result = subprocess.run(
                full_cmd,
                shell=True,  # Using shell to support pipes and shell constructs when provided
                capture_output=True,
                text=True,
            )

            if result.stdout:
                self.logger.info(f"Command stdout:\n{result.stdout.strip()}")
            if result.stderr:
                # stderr is logged as warning to highlight issues without failing by itself
                self.logger.warning(f"Command stderr:\n{result.stderr.strip()}")

            if result.returncode != 0:
                # Non-zero return code indicates failure
                raise Exception(f"Command failed with exit code {result.returncode}")

            self.logger.info("Command executed successfully")

        except Exception as e:
            # Log the exception and re-raise to signal job failure upstream
            self.logger.error(f"Command execution failed: {e}")
            raise
