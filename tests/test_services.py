import tempfile
import unittest

import pandas as pd

from src.ingestion import bulk_ingest
from src.services import generate_recon_report, generate_ror_report
from src.setup import setup_pandas_config
from tests.utils.mock_data import get_mock_master_sql
from tests.utils.setup_tests import setup_test_environment


class TestServices(unittest.TestCase):

    def setUp(self):
        setup = setup_test_environment()
        self.db = setup['db']
        self.ingest_dir = setup['ingest_dir']
        bulk_ingest(self.db, source_folder=self.ingest_dir.name)

        # Setup mock master reference data
        self.mock_master_sql = get_mock_master_sql()
        self.db.execute_script(self.mock_master_sql)

        # Setup output folder
        self.output_folder = tempfile.TemporaryDirectory()

    def test_generate_recon_report(self):
        recon_report_df = generate_recon_report(db=self.db, output_folder=self.output_folder.name)
        expected_recon_df = pd.DataFrame({
            "FUND NAME": ["Applebead", "Belaware"],
            "FINANCIAL TYPE": ["Equities", "Government Bond"],
            "SYMBOL": ["SYY", "JP1234567AB"],
            "PRICE": [73.6, 102.5],
            "REPORTING DATE": ["2023-10-31", "2023-10-31"],
            "REF PRICE": [75.6, 103.0],
            "REF DATE": ["2023-10-30", "2023-10-30"]
        })
        pd.testing.assert_frame_equal(recon_report_df, expected_recon_df)

    def test_generate_ror_report(self):
        ror_report_df = generate_ror_report(db=self.db, output_folder=self.output_folder.name)
        setup_pandas_config()
        expected_ror_df = pd.DataFrame({
            "FUND NAME": ["Applebead"],
            "REPORTING DATE": ["2023-10-31"],
            "ROR": [0.4139414802065404],
        })
        pd.testing.assert_frame_equal(ror_report_df, expected_ror_df)

    def tearDown(self):
        self.db.close_conn()
        self.ingest_dir.cleanup()
        self.output_folder.cleanup()
