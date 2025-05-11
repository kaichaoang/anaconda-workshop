import tempfile
import unittest

import pandas as pd

from src.ingestion import bulk_ingest
from src.services import generate_recon_report, generate_ror_report
from src.setup import setup_pandas_config
from tests.utils.mock_data import get_mock_master_sql
from tests.utils.setup_tests import setup_test_environment


class TestBulkIngest(unittest.TestCase):

    def setUp(self):
        setup = setup_test_environment()
        self.db = setup['db']
        self.ingest_dir = setup['ingest_dir']
        bulk_ingest(self.db, source_folder=self.ingest_dir.name, is_ingest=True)

        # Setup mock master reference data
        self.mock_master_sql = get_mock_master_sql()
        self.db.execute_script(self.mock_master_sql)

        # Setup output folder
        self.output_folder = tempfile.TemporaryDirectory()

    def test_generate_recon_report(self):
        recon_report_df = generate_recon_report(db=self.db, output_folder=self.output_folder.name)
        expected_recon_df = pd.DataFrame({
            "FUND NAME": ["Applebead", "Belaware"],
            "FINANCIAL TYPE": ["Government Bond", "Equities"],
            "SYMBOL": ["JP1103451GC0", "XYZ"],
            "PRICE": [100.85, 50.00],
            "REPORTING DATE": ["2023-10-01", "2023-10-01"],
            "REF PRICE": [100.7, 51.1],
            "REF DATE": ["2023-10-01", "2023-10-01"]
        })
        pd.testing.assert_frame_equal(recon_report_df, expected_recon_df)


    def test_generate_ror_report(self):
        ror_report_df = generate_ror_report(db=self.db, output_folder=self.output_folder.name)
        setup_pandas_config()
        expected_ror_df = pd.DataFrame({
            "FUND NAME": ["Applebead"],
            "REPORTING DATE": ["2023-10-01"],
            "ROR": [0.369915],
        })
        pd.testing.assert_frame_equal(ror_report_df, expected_ror_df)


    def tearDown(self):
        self.db.close_conn()
        self.ingest_dir.cleanup()
        self.output_folder.cleanup()

if __name__ == '__main__':
    unittest.main()