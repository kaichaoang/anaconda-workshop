import re

file_identifier_regex_map = {
    "Applebead": {
        "file_regex": re.compile(r"^Applebead\.([0-9]{2}[-_][0-9]{2}[-_][0-9]{4})(?: breakdown)?\.csv$"),
        "raw_date_format": "%d-%m-%Y"
    },
    "Belaware": {
        "file_regex": re.compile(r"^Belaware\.([0-9]{2}[-_][0-9]{2}[-_][0-9]{4})(?: .*)?\.csv$"),
        "raw_date_format": "%d_%m_%Y"
    },
    "Whitestone": {
        "file_regex": re.compile(r"^Fund Whitestone\.([0-9]{2}[-_][0-9]{2}[-_][0-9]{4})(?: - details)?\.csv$"),
        "raw_date_format": "%d-%m-%Y"
    },
    "Leeder": {
        "file_regex": re.compile(r"^Leeder\.([0-9]{2}[-_][0-9]{2}[-_][0-9]{4})\.csv$"),
        "raw_date_format": "%m_%d_%Y"
    },
    "Magnum": {
        "file_regex": re.compile(r"^Magnum\.([0-9]{2}[-_][0-9]{2}[-_][0-9]{4})\.csv$"),
        "raw_date_format": "%d-%m-%Y"
    },
    "Wallington": {
        "file_regex": re.compile(r"^mend-report Wallington\.([0-9]{2}[-_][0-9]{2}[-_][0-9]{4})(?: .*)?\.csv$"),
        "raw_date_format": "%d_%m_%Y"
    },
    "Gohen": {
        "file_regex": re.compile(r"^Report-of-Gohen\.([0-9]{2}[-_][0-9]{2}[-_][0-9]{4})\.csv$"),
        "raw_date_format": "%m-%d-%Y"
    },
    "Catalysm": {
        "file_regex": re.compile(r"^rpt-Catalysm\.([0-9]{4}[-][0-9]{2}[-][0-9]{2})\.csv$"),
        "raw_date_format": "%Y-%m-%d"
    },
    "Trustmind": {
        "file_regex": re.compile(r"^TT_monthly_Trustmind\.([0-9]{8})\.csv$"),
        "raw_date_format": "%Y%m%d"
    },
    "Virtous": {
        "file_regex": re.compile(r"^Virtous\.([0-9]{2}[-_][0-9]{2}[-_][0-9]{4})(?: - securities)?\.csv$"),
        "raw_date_format": "%m-%d-%Y"
    },
}

columns_to_include = [
    "FINANCIAL TYPE",
    "SYMBOL",
    "SECURITY NAME",
    "SEDOL",
    "ISIN",
    "PRICE",
    "QUANTITY",
    "REALISED P/L",
    "MARKET VALUE"
]

equity_type = "Equities"
bond_type = "Government Bond"

resources_folder = './resources/'

master_data_sql_filename = 'master-reference-sql.sql'
db_path = './resources/master-data.db'

external_funds_zip_path = './resources/external-funds.zip'
external_funds_folder = './resources/external-funds/'

raw_external_funds_table = 'raw_external_funds'

report_folder = './output/'
recon_report_filename = 'recon_report.csv'
ror_report_filename = 'ror_report.csv'