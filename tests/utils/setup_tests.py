import os
import tempfile

import pandas as pd

from src.database import SQLiteDatabase
from src.sql_templates import setup_ingestion_table_query
from tests.utils.mock_data import (
    get_mock_data_applebead,
    get_mock_data_belaware,
)


def setup_test_environment():
    db = SQLiteDatabase(':memory:')
    ingest_dir = tempfile.TemporaryDirectory()

    # Setup mock Applebead data
    mock_data_applebead_df = get_mock_data_applebead()
    mock_csv_path_applebead = os.path.join(ingest_dir.name, 'Applebead.31-10-2023.csv')
    mock_data_applebead_df.to_csv(mock_csv_path_applebead, index=False)

    # Setup mock Belaware data
    mock_data_belaware_df = get_mock_data_belaware()
    mock_csv_path_belaware = os.path.join(ingest_dir.name, 'Belaware.31_10_2023.csv')
    mock_data_belaware_df.to_csv(mock_csv_path_belaware, index=False)

    # Setup mock fund data ingestion
    mock_bulk_ingest_df = pd.concat([mock_data_applebead_df, mock_data_belaware_df], ignore_index=True)
    db.execute_script(setup_ingestion_table_query())

    return {
        'db': db,
        'ingest_dir': ingest_dir,
        'mock_data_applebead_df': mock_data_applebead_df,
        'mock_data_belaware_df': mock_data_belaware_df,
        'mock_bulk_ingest_df': mock_bulk_ingest_df,
        'mock_csv_path_applebead': mock_csv_path_applebead,
        'mock_csv_path_belaware': mock_csv_path_belaware,
    }
