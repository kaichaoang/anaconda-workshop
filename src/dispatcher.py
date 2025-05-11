import argparse

import src.constants as const
from src.ingestion import bulk_ingest
from src.services import generate_recon_report, generate_ror_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run data pipeline tasks.")
    parser.add_argument('--all', action='store_true', help='Run all tasks')
    parser.add_argument('--ingest', action='store_true', help='Ingest external fund data')
    parser.add_argument('--recon', action='store_true', help='Generate reconciliation report')
    parser.add_argument('--ror', action='store_true', help='Generate rate of return report')
    return parser.parse_args()

def run_tasks(db) -> None:
    args = parse_args()

    if args.all:
        bulk_ingest(db=db, source_folder=const.external_funds_folder)
        generate_recon_report(db=db, output_folder=const.report_folder)
        generate_ror_report(db=db, output_folder=const.report_folder)

    if args.ingest:
        bulk_ingest(db=db, source_folder=const.external_funds_folder)

    if args.recon:
        generate_recon_report(db=db, output_folder=const.report_folder)

    if args.ror:
        generate_ror_report(db=db, output_folder=const.report_folder)
