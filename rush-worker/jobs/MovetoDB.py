import sys
from jobs.job import Job



class movetoDB(Job):
    def run(self):
        read_file = open("/home/rush/rush-output/output.txt, r+")
        readlines = read_file.readlines();
        command = f"INSERT INTO Analysis (Name, Attribute, Result) values"
        for line in readlines:
            elements = line.rsplit("\t")
            attribute = elements[0].strip("\"")
            value = elements[1]
            name = "Cause of Death"
            command += f"( '{name}', '{attribute}', {value}),"
        command.rstrip(",")
        command += ";"
        self.logger.info(command)


