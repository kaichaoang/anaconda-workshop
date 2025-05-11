import pytest

from src.utils import extract_fund_and_date

valid_filenames = [
    ("Applebead.30-11-2022 breakdown.csv", ("Applebead", "30-11-2022")),
    ("Belaware.28_02_2023.csv", ("Belaware", "28_02_2023")),
    ('Fund Whitestone.28-02-2023 - details.csv', ("Whitestone", "28-02-2023")),
    ('Leeder.01_31_2023.csv', ("Leeder", "01_31_2023")),
    ('Magnum.28-02-2023.csv', ("Magnum", "28-02-2023")),
    ('mend-report Wallington.30_06_2023.csv', ("Wallington", "30_06_2023")),
    ('Report-of-Gohen.01-31-2023.csv', ("Gohen", "01-31-2023")),
    ('rpt-Catalysm.2022-09-30.csv', ("Catalysm", "2022-09-30")),
    ('TT_monthly_Trustmind.20221031.csv', ("Trustmind", "20221031")),
    ('Virtous.05-31-2023 - securities.csv', ("Virtous", "05-31-2023")),
]

invalid_filenames = [
    "Applebeads.30-11-2022 breakdown.csv",
    "Tt_monthly_Trustmind.20221031.csv"
]

@pytest.mark.parametrize("filename, expected", valid_filenames)
def test_valid_filenames(filename, expected):
    assert extract_fund_and_date(filename) == expected

@pytest.mark.parametrize("filename", invalid_filenames)
def test_invalid_filenames(filename):
    extracted_fund, extracted_date = extract_fund_and_date(filename)
    assert (extracted_fund, extracted_date) == (None, None)