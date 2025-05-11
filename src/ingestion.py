import os

import src.constants as const
from src.database import SQLiteDatabase
from src.sql_templates import ingestion_query, setup_ingestion_table_query
from src.utils import (
    extract_fund_and_date,
    generate_values_for_insertion_from_df,
    read_csv_to_df,
    standardize_datetime,
)


def bulk_ingest(db: SQLiteDatabase, source_folder: str, is_ingest: bool) -> None:
    if is_ingest:
        db.execute_script(setup_ingestion_table_query())

        for filename in os.listdir(source_folder):
            filepath = os.path.join(source_folder, filename)
            ingest(csv_filepath=filepath, filename=filename, db=db)


def ingest(csv_filepath: str, filename: str, db: SQLiteDatabase) -> None:
    print(f"Ingesting: {filename}")

    extracted_fund, extracted_date = extract_fund_and_date(filename)
    print(f"Extracted fund: {extracted_fund} and extracted date: {extracted_date}")
    is_recognised = extracted_fund is not None and extracted_date is not None

    if not is_recognised:
        print(f"File {csv_filepath} is not recognised, skipping ingestion")
        return

    df = read_csv_to_df(csv_filepath)

    enrichment_values = {
        "FUND NAME": extracted_fund,
        "REPORTING DATE": standardize_datetime(extracted_fund, extracted_date)
    }

    values = generate_values_for_insertion_from_df(df, enrichment_values, const.columns_to_include)
    query = ingestion_query(values)

    db.execute_script(query)



