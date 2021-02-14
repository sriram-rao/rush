import logging
import string
import threading


class SqlRepo:
    __instance = None

    @staticmethod
    def get_instance(self):
        if SqlRepo.__instance is None:
            SqlRepo.__instance = SqlRepo()
        return SqlRepo.__instance

    def __init__(self):
        context = threading.current_thread().__dict__
        logger = logging.getLogger(context["name"])
        if SqlRepo.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            SqlRepo.__instance = self

    def fetch_entity(self, sql: string):
        pass

    def fetch_entities(self, sql: string):
        pass

    def execute(self, command: string):
        pass
