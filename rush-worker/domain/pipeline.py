class Pipeline:
    def __init__(self, row: tuple):
        self.name = row[0]
        self.jobs = [JobDefinition(job_def, self.name) for job_def in row[1]["jobs"]]  # list of job definitions
        # we will start with pipelines having a single job so that we can handle child jobs later


class JobDefinition:
    def __init__(self, jobdef: dict, pipeline: str):
        self.pipeline = pipeline
        self.name = jobdef["name"]
        self.class_name = jobdef["class_name"]
        self.timeout_minutes = jobdef["timeout_minutes"]
        self.retry_after_minutes = jobdef["retry_gap_minutes"]
        self.retry_limit = jobdef["retry_limit"]
