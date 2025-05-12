import tempfile
import unittest

import pandas as pd

import src.constants as const
from src.ingestion import bulk_ingest
from src.sql_templates import (
    get_bonds_price_with_nearest_date_query,
    get_bonds_recon_report_with_fund_name_and_reporting_date_query,
    get_distinct_fund_name_and_reporting_date_query,
    get_distinct_reporting_date_query,
    get_equity_price_with_nearest_date_query,
    get_equity_recon_report_with_fund_name_and_reporting_date_query,
    get_rate_of_return_query,
    get_recon_report_query,
    ingestion_query,
    setup_ingestion_table_query,
)
from tests.utils.mock_data import get_mock_master_sql
from tests.utils.setup_tests import setup_test_environment


class TestSQLTemplates(unittest.TestCase):

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

    def test_setup_ingestion_table_query(self):
        self.db.execute_script(setup_ingestion_table_query())

        columns_df = self.db.fetch_as_df(f"PRAGMA table_info('{const.raw_external_funds_table}');")
        expected_columns = [
            ("FINANCIAL TYPE", "TEXT"),
            ("SYMBOL", "TEXT"),
            ("SECURITY NAME", "TEXT"),
            ("SEDOL", "TEXT"),
            ("ISIN", "TEXT"),
            ("PRICE", "REAL"),
            ("QUANTITY", "REAL"),
            ("REALISED P/L", "REAL"),
            ("MARKET VALUE", "REAL"),
            ("FUND NAME", "TEXT"),
            ("REPORTING DATE", "TEXT"),
        ]

        # Verify the column names and types, (name, type)
        actual_columns = [(row['name'], row['type']) for _, row in columns_df.iterrows()]

        assert actual_columns == expected_columns

    def test_ingestion_query(self):
        values = "('Equity', 'TEST_SYMBOL', 'Test Inc.', 'B0001234', 'US0378331005', 150.0, 100, 500.0, 15000.0, 'Test Fund', '2025-05-12')"
        self.db.execute_script(ingestion_query(values))

        actual_df = self.db.fetch_as_df(f"SELECT * FROM '{const.raw_external_funds_table}' WHERE SYMBOL = 'TEST_SYMBOL';")
        expected_df = pd.DataFrame({
            "FINANCIAL TYPE": ["Equity"],
            "SYMBOL": ["TEST_SYMBOL"],
            "SECURITY NAME": ["Test Inc."],
            "SEDOL": ["B0001234"],
            "ISIN": ["US0378331005"],
            "PRICE": [150.0],
            "QUANTITY": [100.0],
            "REALISED P/L": [500.0],
            "MARKET VALUE": [15000.0],
            "FUND NAME": ["Test Fund"],
            "REPORTING DATE": ["2025-05-12"]
        })

        pd.testing.assert_frame_equal(actual_df, expected_df)

    def test_get_equity_price_with_nearest_date_query(self):
        actual_df = self.db.fetch_as_df(get_equity_price_with_nearest_date_query("2023-10-31"))
        expected_df = pd.DataFrame({
            "REF SYMBOL": ["SYY", "XYZ"],
            "REF COUNTRY": ["US", "US"],
            "REF SECURITY NAME": ["Sysco", "Company XYZ"],
            "REF SECTOR": ["Consumer Staples", "Consumer Staples"],
            "REF INDUSTRY": ["Food Distributors", "Food Distributors"],
            "REF CURRENCY": ["USD", "USD"],
            "REF DATE": ["2023-10-30", "2023-10-30"],
            "REF PRICE": [75.6, 50.0]
        })

        pd.testing.assert_frame_equal(actual_df, expected_df)

    def test_get_equity_recon_report_with_fund_name_and_reporting_date_query(self):
        actual_df = self.db.fetch_as_df(get_equity_recon_report_with_fund_name_and_reporting_date_query("Applebead","2023-10-31"))
        expected_df = pd.DataFrame({
            "FUND NAME": ["Applebead"],
            "FINANCIAL TYPE": ["Equities"],
            "SYMBOL": ["SYY"],
            "PRICE": [73.6],
            "REPORTING DATE": ["2023-10-31"],
            "REF PRICE": [75.6],
            "REF DATE": ["2023-10-30"]
        })

        pd.testing.assert_frame_equal(actual_df, expected_df)

        actual_df = self.db.fetch_as_df(get_equity_recon_report_with_fund_name_and_reporting_date_query("Belaware","2023-10-31"))
        expected_df = pd.DataFrame(columns=[
            "FUND NAME",
            "FINANCIAL TYPE",
            "SYMBOL",
            "PRICE",
            "REPORTING DATE",
            "REF PRICE",
            "REF DATE"
        ])

        pd.testing.assert_frame_equal(actual_df, expected_df)

    def test_get_bonds_price_with_nearest_date_query(self):
        actual_df = self.db.fetch_as_df(get_bonds_price_with_nearest_date_query("2023-10-31"))
        expected_df = pd.DataFrame({
            "REF SECURITY NAME": [
                "JAPAN (10 YEAR ISSUE) 0.1 % 12/20/2026 ",
                "JAPAN (5 YEAR ISSUE) 0.5 % 12/20/2025 "
            ],
            "REF ISIN": ["JP1103451GC0", "JP1234567AB"],
            "REF SEDOL": ["BYYNK79", "BYYNK80"],
            "REF COUNTRY": ["JP", "JP"],
            "REF COUPON": [0.1, 0.1],
            "REF MATURITY DATE": ["20/12/2026", "20/12/2026"],
            "REF COUPON FREQUENCY": ["Levery 6 month", "Levery 6 month"],
            "REF SECTOR": ["Treasury", "Treasury"],
            "REF CURRENCY": ["USD", "USD"],
            "REF DATE": ["2023-10-30", "2023-10-30"],
            "REF PRICE": [100.85, 103.00]
        })

        pd.testing.assert_frame_equal(actual_df, expected_df)

    def test_get_bonds_recon_report_with_fund_name_and_reporting_date_query(self):
        actual_df = self.db.fetch_as_df(get_bonds_recon_report_with_fund_name_and_reporting_date_query("Applebead","2023-10-31"))
        expected_df = pd.DataFrame(columns=[
            "FUND NAME",
            "FINANCIAL TYPE",
            "SYMBOL",
            "PRICE",
            "REPORTING DATE",
            "REF PRICE",
            "REF DATE"
        ])

        pd.testing.assert_frame_equal(actual_df, expected_df)

        actual_df = self.db.fetch_as_df(get_bonds_recon_report_with_fund_name_and_reporting_date_query("Belaware","2023-10-31"))
        expected_df = pd.DataFrame({
            "FUND NAME": ["Belaware"],
            "FINANCIAL TYPE": ["Government Bond"],
            "SYMBOL": ["JP1234567AB"],
            "PRICE": [102.5],
            "REPORTING DATE": ["2023-10-31"],
            "REF PRICE": [103.0],
            "REF DATE": ["2023-10-30"]
        })

        pd.testing.assert_frame_equal(actual_df, expected_df)

    def test_get_recon_report_query(self):
        actual_df = self.db.fetch_as_df(get_recon_report_query("Applebead","2023-10-31"))
        expected_df = pd.DataFrame({
            "FUND NAME": ["Applebead"],
            "FINANCIAL TYPE": ["Equities"],
            "SYMBOL": ["SYY"],
            "PRICE": [73.6],
            "REPORTING DATE": ["2023-10-31"],
            "REF PRICE": [75.6],
            "REF DATE": ["2023-10-30"]
        })

        pd.testing.assert_frame_equal(actual_df, expected_df)

        actual_df = self.db.fetch_as_df(get_recon_report_query("Belaware","2023-10-31"))
        expected_df = pd.DataFrame({
            "FUND NAME": ["Belaware"],
            "FINANCIAL TYPE": ["Government Bond"],
            "SYMBOL": ["JP1234567AB"],
            "PRICE": [102.5],
            "REPORTING DATE": ["2023-10-31"],
            "REF PRICE": [103.0],
            "REF DATE": ["2023-10-30"]
        })

        pd.testing.assert_frame_equal(actual_df, expected_df)

    def test_get_distinct_fund_name_and_reporting_date_query(self) -> str:
        actual_df = self.db.fetch_as_df(get_distinct_fund_name_and_reporting_date_query())
        expected_df = pd.DataFrame({
            "FUND NAME": ["Applebead", "Belaware"],
            "REPORTING DATE": ["2023-10-31", "2023-10-31"]
        })

        pd.testing.assert_frame_equal(actual_df, expected_df)

    def test_get_distinct_reporting_date_query(self) -> str:
        actual_df = self.db.fetch_as_df(get_distinct_reporting_date_query())
        expected_df = pd.DataFrame({
            "REPORTING DATE": ["2023-10-31"]
        })

        pd.testing.assert_frame_equal(actual_df, expected_df)

    def test_get_rate_of_return_query(self):
        actual_df = self.db.fetch_as_df(get_rate_of_return_query("2023-10-31"))
        expected_df = pd.DataFrame({
            "FUND NAME": ["Applebead", "Belaware"],
            "REPORTING DATE": ["2023-10-31", "2023-10-31"],
            "ROR": [0.413941, 0.393229]
        })

        pd.testing.assert_frame_equal(actual_df, expected_df)

    def tearDown(self):
        self.db.close_conn()
        self.ingest_dir.cleanup()
        self.output_folder.cleanup()
