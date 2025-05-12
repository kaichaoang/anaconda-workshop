import logging
import os

import pandas as pd

import src.constants as const
from src.database import SQLiteDatabase
from src.sql_templates import (
    get_bonds_recon_report_with_fund_name_and_reporting_date_query,
    get_distinct_fund_name_and_reporting_date_query,
    get_distinct_reporting_date_query,
    get_equity_recon_report_with_fund_name_and_reporting_date_query,
    get_rate_of_return_query,
    get_recon_report_query,
)
from src.utils import get_best_performing_funds


def generate_recon_report_for_equity(db: SQLiteDatabase) -> pd.DataFrame:
    fund_name_and_reporting_date = db.fetch_as_df(get_distinct_fund_name_and_reporting_date_query())

    recon_df = pd.DataFrame()

    for _, row in fund_name_and_reporting_date.iterrows():
        fund_name, reporting_date = row['FUND NAME'], row['REPORTING DATE']
        logging.info(f"Running EQUITY recon report for fund name: {fund_name}, reporting date: {reporting_date}")

        recon_report_equity_query = get_equity_recon_report_with_fund_name_and_reporting_date_query(fund_name,
                                                                                                      reporting_date)
        recon_equity_results = db.fetch_as_df(recon_report_equity_query)

        if not recon_equity_results.empty:
            logging.warning(f"Require EQUITY reconciliation for fund name: {fund_name}, reporting date: {reporting_date}!")
            recon_df = pd.concat([recon_df, recon_equity_results], ignore_index=True)

    logging.info("Completed recon report for EQUITY")

    return recon_df.reset_index(drop=True)

def generate_recon_report_for_bonds(db: SQLiteDatabase) -> pd.DataFrame:
    fund_name_and_reporting_date = db.fetch_as_df(get_distinct_fund_name_and_reporting_date_query())

    recon_df = pd.DataFrame()

    for _, row in fund_name_and_reporting_date.iterrows():
        fund_name, reporting_date = row['FUND NAME'], row['REPORTING DATE']
        logging.info(f"Running BONDS recon report for fund name: {fund_name}, reporting date: {reporting_date}")

        recon_report_bonds_query = get_bonds_recon_report_with_fund_name_and_reporting_date_query(fund_name, reporting_date)
        recon_bonds_results = db.fetch_as_df(recon_report_bonds_query)

        if not recon_bonds_results.empty:
            logging.warning(f"Require BONDS reconciliation for fund name: {fund_name}, reporting date: {reporting_date}!")
            recon_df = pd.concat([recon_df, recon_bonds_results], ignore_index=True)

    logging.info("Completed recon report for BONDS")

    return recon_df.reset_index(drop=True)

def generate_recon_report(db: SQLiteDatabase, output_folder: str) -> pd.DataFrame:
    fund_name_and_reporting_date = db.fetch_as_df(get_distinct_fund_name_and_reporting_date_query())

    recon_df = pd.DataFrame()

    for _, row in fund_name_and_reporting_date.iterrows():
        fund_name, reporting_date = row['FUND NAME'], row['REPORTING DATE']
        logging.info(f"Running recon report for fund name: {fund_name}, reporting date: {reporting_date}")

        recon_report_query = get_recon_report_query(fund_name, reporting_date)
        recon_results = db.fetch_as_df(recon_report_query)

        if not recon_results.empty:
            logging.warning(f"Require reconciliation for fund name: {fund_name}, reporting date: {reporting_date}!")
            recon_df = pd.concat([recon_df, recon_results], ignore_index=True)

    logging.info("Completed recon report")

    os.makedirs(output_folder, exist_ok=True)
    recon_report_path = os.path.join(output_folder, const.recon_report_filename)
    recon_df.to_csv(recon_report_path, index=False)
    logging.info(f"Saved recon report to {const.recon_report_filename}")

    return recon_df.reset_index(drop=True)

def generate_ror_report(db: SQLiteDatabase, output_folder: str) -> pd.DataFrame:
    reporting_dates = db.fetch_as_df(get_distinct_reporting_date_query())

    ror_df = pd.DataFrame()

    for _, row in reporting_dates.iterrows():
        reporting_date = row['REPORTING DATE']
        logging.info(f"Running ROR report for reporting date: {reporting_date}")

        ror_query = get_rate_of_return_query(reporting_date=reporting_date)
        ror_results = db.fetch_as_df(ror_query)

        ror_df = pd.concat([ror_df, ror_results], ignore_index=True)

    logging.info("Completed ror report")
    best_performing_funds_df = get_best_performing_funds(ror_df)

    os.makedirs(output_folder, exist_ok=True)
    ror_report_path = os.path.join(output_folder, const.ror_report_filename)
    best_performing_funds_df.to_csv(ror_report_path, index=False)
    logging.info(f"Saved ROR report to {const.ror_report_filename}")

    return best_performing_funds_df
