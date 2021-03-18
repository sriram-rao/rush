import sys
from jobs.job import Job
from repository.sqlRepo import SqlRepo


class movetoDB(Job):
    def run(self):
        read_file = open("/home/rush/rush-output/output.txt", "r+")
        readlines = read_file.readlines()
        command = f"INSERT INTO Analysis (Name, Attribute, Result) values "
        for line in readlines:
            elements = line.strip().rsplit("\t")
            attribute = elements[0].strip("\"")
            value = elements[1]
            name = "Cause of Death"
            command += f"( '{name}', '{attribute}', {value}),"
        command = command.rstrip(",")
        command += ";"
        self.logger.info(command)
        SqlRepo().execute(command)
        self.logger.info("Insert complete")


