class Pipeline:
    def __init__(self):
        self.name = ""
        self.jobs = []  # list of job definitions
        # we will start with pipelines having a single job so that we can handle child jobs later


class JobDefinition:
    def __init__(self):
        self.name = ""
        self.class_name = ""
        self.timeout_minutes = 10
        self.retry_gap_minutes = 5
        self.retry_limit = 2
