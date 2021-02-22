from jobs.job import Job


class CompleteJob(Job):

    def run(self):
        # Update the job results to database
        # Set last seen entry under my name in worker table
        pass
