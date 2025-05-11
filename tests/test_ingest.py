import unittest

import pandas as pd

import src.constants as const
from src.ingestion import bulk_ingest, ingest
from tests.utils.setup_tests import setup_test_environment


class TestBulkIngest(unittest.TestCase):

    def setUp(self):
        setup = setup_test_environment()
        self.db = setup['db']
        self.ingest_dir = setup['ingest_dir']
        self.mock_bulk_ingest_df = setup['mock_bulk_ingest_df']
        self.mock_csv_path_belaware = setup['mock_csv_path_belaware']
        self.mock_data_belaware_df = setup['mock_data_belaware_df']

    def test_bulk_ingest(self):
        bulk_ingest(self.db, source_folder=self.ingest_dir.name, is_ingest=True)

        result_df = self.db.fetch_as_df(f"SELECT * FROM {const.raw_external_funds_table};")
        pd.testing.assert_frame_equal(result_df, self.mock_bulk_ingest_df)

    def test_ingest(self):
        ingest(self.mock_csv_path_belaware, "Belaware.01_10_2023.csv", self.db)

        result_df = self.db.fetch_as_df(f"SELECT * FROM {const.raw_external_funds_table} as ef WHERE ef.'FUND NAME' = 'Belaware';")
        pd.testing.assert_frame_equal(result_df, self.mock_data_belaware_df)

    def tearDown(self):
        self.db.close_conn()
        self.ingest_dir.cleanup()

if __name__ == '__main__':
    unittest.main()