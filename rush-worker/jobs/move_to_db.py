from jobs.job import Job


class MoveToDB(Job):
    def run(self):
        output_file = self.params.get("output_file")
        name = self.params.get("analysis_name")
        read_file = open(f"/home/rush/rush-output/{output_file}, r+")
        lines = read_file.readlines()
        command = f"INSERT INTO Analysis (Name, Attribute, Result) values "
        for line in lines:
            elements = line.rsplit("\t")
            attribute = elements[0].strip("\"")
            value = elements[1]
            command += f"( '{name}', '{attribute}', {value}),"
        command.rstrip(",")
        command += ";"
        self.logger.info(command)
