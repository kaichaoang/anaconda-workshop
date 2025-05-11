from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.constants import file_identifier_regex_map


def read_sql_file(file_path: str) -> str:
    with open(file_path, 'r') as file:
        sql_commands = file.read()

    return sql_commands

def read_csv_to_df(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    return df

def extract_fund_and_date(filename: str) -> Tuple[Optional[str], Optional[str]]:
    extracted_fund, extracted_date = None, None

    for fund, pattern in file_identifier_regex_map.items():
        match = pattern["file_regex"].match(filename)
        if match:
            extracted_date = match.group(1)
            extracted_fund = fund
            break

    return extracted_fund, extracted_date

def standardize_datetime(fund: str, date_str: str) -> str:
    raw_date_format = file_identifier_regex_map[fund]["raw_date_format"]
    date_obj = datetime.strptime(date_str, raw_date_format)
    standardized_date = date_obj.strftime("%Y-%m-%d")

    return standardized_date

def generate_values_for_insertion_from_df(df: pd.DataFrame, enrichment: Dict[str, str], columns: List[str]) -> str:
    values_list = []

    for _, row in df.iterrows():
        row_values = []

        for column in columns:
            if column in df.columns:
                value = row[column]
                if pd.isna(value):
                    row_values.append("NULL")
                elif isinstance(value, str):
                    value = value.replace("'", "''")
                    row_values.append(f"'{value}'")
                else:
                    row_values.append(str(value))
            else:
                row_values.append("NULL")

        for col, const_value in enrichment.items():
            row_values.append(f"'{const_value}'")

        values_tuple = "(" + ", ".join(row_values) + ")"
        values_list.append(values_tuple)

    return ", ".join(values_list)

def get_best_performing_funds(df):
    best_performing_indices = df.groupby('REPORTING DATE')['ROR'].idxmax()
    best_performing_funds = df.loc[best_performing_indices]
    print("Successfully got the best performing funds")
    return best_performing_funds