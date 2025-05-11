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
    ("Applebead", "28-13-2023"),  # Invalid month
    ("Belaware", "28_02_23"),      # Invalid year format
    ("Whitestone", "28-02-2023-01"),  # Extra segment
    ("Leeder", "02-28-2023"),      # Incorrect format for Leeder
    ("Magnum", "28-02-2023-abc"),  # Invalid characters
    ("Wallington", "28_02_2023_01"),  # Extra segment
    ("Gohen", "2023-28-02"),        # Incorrect order
    ("Catalysm", "2023/02/28"),    # Invalid separator
    ("Trustmind", "2023-02-28-01"), # Extra segment
    ("Virtous", "02/28/2023"),      # Invalid separator
])
def test_format_date_invalid(fund, input_date):
    with pytest.raises(ValueError):
        standardize_datetime(fund, input_date)