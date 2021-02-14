from domain.job import Job


class DummyJob(Job):

    def run(self):
        self.logger.info("I'm a dummy")
        self.logger.info("I do nothing")
