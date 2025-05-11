import src.constants as const
from src.ingestion import bulk_ingest
from src.services import generate_recon_report, generate_ror_report
from src.setup import init

db = init(external_funds_folder=const.external_funds_folder, db_path=const.db_path)
bulk_ingest(db=db, source_folder=const.external_funds_folder, is_ingest=True)
recon_df = generate_recon_report(db=db, output_folder=const.report_folder)
ror_df = generate_ror_report(db=db, output_folder=const.report_folder)
