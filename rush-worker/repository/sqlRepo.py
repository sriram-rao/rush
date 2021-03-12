import logging
import string
from threading import current_thread
import psycopg2


class SqlRepo:
    def __init__(self):
        # logger = logging.getLogger(current_thread().name)
        self.connection = psycopg2.connect(
            host="localhost",
            database="rushdb",
            user="sriramrao",
            password="")

    def fetch_entity(self, sql: string):
        cursor = self.connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        return result

    def fetch_entities(self, sql: string):
        cursor = self.connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def execute(self, command: string) -> int:
        cursor = self.connection.cursor()
        cursor.execute(command)
        row_count = cursor.rowcount
        self.connection.commit()
        cursor.close()
        return row_count
