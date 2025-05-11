import logging
import os
import shutil

import pandas as pd

import src.constants as const
from src.database import SQLiteDatabase


def init(external_funds_folder: str, db_path: str) -> SQLiteDatabase:
    setup_logging()
    setup_pandas_config()

    if not os.path.exists(external_funds_folder):
        logging.info("External funds folder does not exist, extracting...")
        shutil.unpack_archive(const.external_funds_zip_path, const.resources_folder)
        logging.info("Successfully extracted all external funds.")
    else:
        logging.info("External funds folder already exists, skipping extraction.")

    db = SQLiteDatabase(db_path=db_path)
    ingest_master_data(db=db)

    return db

def setup_pandas_config() -> None:
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )

def ingest_master_data(db: SQLiteDatabase) -> None:
    master_data_filepath = os.path.join(const.resources_folder, const.master_data_sql_filename)
    with open(master_data_filepath, "r") as f:
        query = f.read()
    db.execute_script(query)
