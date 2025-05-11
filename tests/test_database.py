import unittest

import pandas as pd

from src.database import SQLiteDatabase


class TestSQLiteDatabase(unittest.TestCase):
    def setUp(self):
        self.db = SQLiteDatabase(':memory:')
        self.create_test_table()

    def create_test_table(self):
        create_table_query = """
        CREATE TABLE test (
            id INTEGER PRIMARY KEY,
            name TEXT
        );
        """
        self.db.execute_script(create_table_query)

    def test_execute_script(self):
        query = "INSERT INTO test (name) VALUES ('Alice');"
        self.db.execute_script(query)

        result_df = self.db.fetch_as_df("SELECT * FROM test;")
        expected_df = pd.DataFrame({'id': [1], 'name': ['Alice']})
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_fetch_as_df(self):
        insert_query = "INSERT INTO test (name) VALUES ('Alice'), ('Bob');"
        self.db.execute_script(insert_query)

        result_df = self.db.fetch_as_df("SELECT * FROM test;")
        expected_df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_close_conn(self):
        self.db.close_conn()
        self.assertIsNone(self.db.conn)

    def tearDown(self):
        self.db.close_conn()

if __name__ == '__main__':
    unittest.main()