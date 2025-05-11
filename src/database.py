import sqlite3

import pandas as pd


class SQLiteDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.set_connection()

    def set_connection(self):
        conn = sqlite3.connect(self.db_path)
        self.conn = conn

    def execute_script(self, query: str):
        cursor = self.conn.cursor()
        cursor.executescript(query)
        self.conn.commit()

    def fetch_as_df(self, query: str) -> pd.DataFrame:
        cursor = self.conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        df = pd.DataFrame(results, columns=column_names)

        return df

    def close_conn(self):
        if self.conn:
            self.conn.close()
            self.conn = None
