import pandas as pd
import pytest

from src.utils import get_best_performing_funds

df = pd.DataFrame({
    "FUND NAME": ["Applebead", "Belaware", "Whitestone",
                  "Applebead", "Belaware", "Whitestone",
                  "Applebead", "Belaware", "Whitestone"],
    "REPORTING DATE": ["2023-02-28", "2023-02-28", "2023-02-28",
                       "2023-04-30", "2023-04-30", "2023-04-30",
                       "2023-06-30", "2023-06-30", "2023-06-30"],
    "ROR": [0.05032934739066423, 0.03013927351712965, 0.022760347384142687,
            0.07937243983123664, 0.06074507529949916, 0.061529431480515165,
            0.09865816341949812, 0.09680936789847615, 0.09885719415718616]
})

@pytest.mark.parametrize("expected_results", [
    {
        '2023-02-28': ('Applebead', 0.05032934739066423),
        '2023-04-30': ('Applebead', 0.07937243983123664),
        '2023-06-30': ('Whitestone', 0.09885719415718616),
    }
])
def test_get_best_performing_funds(expected_results):
    best_performing_funds = get_best_performing_funds(df)

    for date, (fund_name, ror) in expected_results.items():
        row = best_performing_funds[best_performing_funds['REPORTING DATE'] == date]
        assert row['FUND NAME'].values[0] == fund_name
        assert row['ROR'].values[0] == ror
