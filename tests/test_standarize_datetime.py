import pytest

from src.utils import standardize_datetime


@pytest.mark.parametrize("fund, input_date, expected_output", [
    ("Applebead", "28-02-2023", "2023-02-28"),
    ("Belaware", "28_02_2023", "2023-02-28"),
    ("Whitestone", "28-02-2023", "2023-02-28"),
    ("Leeder", "02_28_2023", "2023-02-28"),
    ("Magnum", "28-02-2023", "2023-02-28"),
    ("Wallington", "28_02_2023", "2023-02-28"),
    ("Gohen", "02-28-2023", "2023-02-28"),
    ("Catalysm", "2023-02-28", "2023-02-28"),
    ("Trustmind", "20230228", "2023-02-28"),
    ("Virtous", "02-28-2023", "2023-02-28"),
])
def test_format_date_valid(fund, input_date, expected_output):
    assert standardize_datetime(fund, input_date) == expected_output

@pytest.mark.parametrize("fund, input_date", [
    ("Applebead", "28-13-2023"),
    ("Belaware", "28_02_23"),
    ("Whitestone", "28-02-2023-01"),
    ("Leeder", "02-28-2023"),
    ("Magnum", "28-02-2023-abc"),
    ("Wallington", "28_02_2023_01"),
    ("Gohen", "2023-28-02"),
    ("Catalysm", "2023/02/28"),
    ("Trustmind", "2023-02-28-01"),
    ("Virtous", "02/28/2023"),
])
def test_format_date_invalid(fund, input_date):
    with pytest.raises(ValueError):
        standardize_datetime(fund, input_date)
