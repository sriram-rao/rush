import logging
import string
import threading
import psycopg2


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
        self.conn = psycopg2.connect(
            host="localhost",
            database="suppliers",
            user="postgres",
            password="Abcd1234")
        if SqlRepo.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            SqlRepo.__instance = self

    def fetch_entity(self, sql: string):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        return result

    def fetch_entities(self, sql: string):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def execute(self, command: string):
        cursor = self.conn.cursor()
        cursor.execute(command)
        cursor.commit()
        cursor.close()
