import psycopg2
from psycopg2.extensions import cursor as Cursor, connection as Connection


class SqlRepo:
    def __init__(self):
        # logger = logging.getLogger(current_thread().name)
        self.connection: Connection = psycopg2.connect(
            host="localhost",
            database="rushdb",
            user="sriramrao",
            password="")
        self.active_cursor: Cursor | None = None

    def fetch_entity(self, sql: str):
        current_cursor = self.get_cursor()
        current_cursor.execute(sql)
        result = current_cursor.fetchone()
        self.complete_cursor(current_cursor)
        return result

    def fetch_entities(self, sql: str):
        cursor = self.get_cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        self.complete_cursor(cursor)
        return result

    def execute(self, command: str) -> int:
        cursor = self.get_cursor()
        cursor.execute(command)
        row_count = cursor.rowcount
        if self.active_cursor is None:
            self.connection.commit()
        self.complete_cursor(cursor)
        return row_count

    def begin_transaction(self):
        self.active_cursor = self.connection.cursor()

    def commit_transaction(self):
        self.connection.commit()
        if not self.active_cursor:
            return
        self.active_cursor.close()
        self.active_cursor = None

    def rollback_transaction(self):
        self.connection.rollback()
        if not self.active_cursor:
            return
        self.active_cursor.close()
        self.active_cursor = None

    def get_cursor(self) -> Cursor:
        if self.active_cursor is None:
            return self.connection.cursor()
        return self.active_cursor

    def complete_cursor(self, active_cursor: psycopg2.extensions.cursor) -> None:
        if self.active_cursor is not None:
            return
        active_cursor.close()

