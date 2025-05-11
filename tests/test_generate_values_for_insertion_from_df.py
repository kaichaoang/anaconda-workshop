import pandas as pd
import pytest

from src.utils import generate_values_for_insertion_from_df


@pytest.mark.parametrize("df, enrichment, columns, expected_output", [
    (
        pd.DataFrame({
            "FINANCIAL TYPE": ["Type1"],
            "SYMBOL": ["SYM1"],
            "SECURITY NAME": ["Security1"],
            "SEDOL": ["SEDOL1"],
            "ISIN": ["ISIN1"],
            "PRICE": [100.0],
            "QUANTITY": [10],
            "REALISED P/L": [5.0],
            "MARKET VALUE": [1000.0]
        }),
        {"enrichment_col": "enrichment_value"},
        [
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY NAME",
            "SEDOL",
            "ISIN",
            "PRICE",
            "QUANTITY",
            "REALISED P/L",
            "MARKET VALUE"
        ],
        "('Type1', 'SYM1', 'Security1', 'SEDOL1', 'ISIN1', 100.0, 10, 5.0, 1000.0, 'enrichment_value')"
    ),
])
def test_all_columns_present(df, enrichment, columns, expected_output):
    result = generate_values_for_insertion_from_df(df, enrichment, columns)
    assert result == expected_output

@pytest.mark.parametrize("df, enrichment, columns, expected_output", [
    (
        pd.DataFrame({
            "FINANCIAL TYPE": ["Type1"],
            "SYMBOL": ["SYM1"],
            "SECURITY NAME": ["Security1"],
            "ISIN": ["ISIN1"],
            "PRICE": [100.0],
            "QUANTITY": [10],
            "REALISED P/L": [5.0],
            "MARKET VALUE": [1000.0]
        }),
        {"enrichment_col": "enrichment_value"},
        [
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY NAME",
            "SEDOL",
            "ISIN",
            "PRICE",
            "QUANTITY",
            "REALISED P/L",
            "MARKET VALUE"
        ],
        "('Type1', 'SYM1', 'Security1', NULL, 'ISIN1', 100.0, 10, 5.0, 1000.0, 'enrichment_value')"
    ),
])
def test_missing_columns(df, enrichment, columns, expected_output):
    result = generate_values_for_insertion_from_df(df, enrichment, columns)
    assert result == expected_output

@pytest.mark.parametrize("df, enrichment, columns, expected_output", [
    (
        pd.DataFrame(columns=[
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY NAME",
            "SEDOL",
            "ISIN",
            "PRICE",
            "QUANTITY",
            "REALISED P/L",
            "MARKET VALUE"
        ]),
        {"enrichment_col": "enrichment_value"},
        [
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY NAME",
            "SEDOL",
            "ISIN",
            "PRICE",
            "QUANTITY",
            "REALISED P/L",
            "MARKET VALUE"
        ],
        ""
    ),
])
def test_empty_dataframe(df, enrichment, columns, expected_output):
    result = generate_values_for_insertion_from_df(df, enrichment, columns)
    assert result == expected_output

@pytest.mark.parametrize("df, enrichment, columns, expected_output", [
    (
        pd.DataFrame({
            "FINANCIAL TYPE": ["Type1"],
            "SYMBOL": [None],
            "SECURITY NAME": ["Security1"],
            "SEDOL": ["SEDOL1"],
            "ISIN": [None],
            "PRICE": [100.0],
            "QUANTITY": [None],
            "REALISED P/L": [5.0],
            "MARKET VALUE": [1000.0]
        }),
        {"enrichment_col": "enrichment_value"},
        [
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY NAME",
            "SEDOL",
            "ISIN",
            "PRICE",
            "QUANTITY",
            "REALISED P/L",
            "MARKET VALUE"
        ],
        "('Type1', NULL, 'Security1', 'SEDOL1', NULL, 100.0, NULL, 5.0, 1000.0, 'enrichment_value')"
    ),
])
def test_nan_values(df, enrichment, columns, expected_output):
    result = generate_values_for_insertion_from_df(df, enrichment, columns)
    assert result == expected_output

@pytest.mark.parametrize("df, enrichment, columns, expected_output", [
    (
        pd.DataFrame({
            "FINANCIAL TYPE": ["Type1"],
            "SYMBOL": ["SYM1"],
            "SECURITY NAME": ["Security1"]
        }),
        {"enrichment_col": "enrichment_value"},
        [
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY NAME"
        ],
        "('Type1', 'SYM1', 'Security1', 'enrichment_value')"
    ),
])
def test_fewer_columns(df, enrichment, columns, expected_output):
    result = generate_values_for_insertion_from_df(df, enrichment, columns)
    assert result == expected_output

@pytest.mark.parametrize("df, enrichment, columns, expected_output", [
    (
        pd.DataFrame({
            "FINANCIAL TYPE": ["Type1", "Type2"],
            "SYMBOL": ["SYM1", "SYM2"],
            "SECURITY NAME": ["Security1", "Security2"],
            "SEDOL": ["SEDOL1", "SEDOL2"],
            "ISIN": ["ISIN1", "ISIN2"],
            "PRICE": [100.0, 200.0],
            "QUANTITY": [10, 20],
            "REALISED P/L": [5.0, 10.0],
            "MARKET VALUE": [1000.0, 2000.0]
        }),
        {"enrichment_col": "enrichment_value"},
        [
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY NAME",
            "SEDOL",
            "ISIN",
            "PRICE",
            "QUANTITY",
            "REALISED P/L",
            "MARKET VALUE"
        ],
        "('Type1', 'SYM1', 'Security1', 'SEDOL1', 'ISIN1', 100.0, 10, 5.0, 1000.0, 'enrichment_value'), " \
        "('Type2', 'SYM2', 'Security2', 'SEDOL2', 'ISIN2', 200.0, 20, 10.0, 2000.0, 'enrichment_value')"
    ),
])
def test_multiple_rows(df, enrichment, columns, expected_output):
    result = generate_values_for_insertion_from_df(df, enrichment, columns)
    assert result == expected_output

@pytest.mark.parametrize("df, enrichment, columns, expected_output", [
    (
        pd.DataFrame({
            "FINANCIAL TYPE": ["Type1"],
            "SYMBOL": ["SYM1"],
            "SECURITY NAME": ["Security1"],
            "SEDOL": ["SEDOL1"],
            "ISIN": ["ISIN1"],
            "PRICE": [100.0],
            "QUANTITY": [10],
            "REALISED P/L": [5.0],
            "MARKET VALUE": [1000.0]
        }),
        {"enrichment_col1": "value1", "enrichment_col2": "value2"},
        [
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY NAME",
            "SEDOL",
            "ISIN",
            "PRICE",
            "QUANTITY",
            "REALISED P/L",
            "MARKET VALUE"
        ],
        "('Type1', 'SYM1', 'Security1', 'SEDOL1', 'ISIN1', 100.0, 10, 5.0, 1000.0, 'value1', 'value2')"
    ),
])
def test_different_enrichment_values(df, enrichment, columns, expected_output):
    result = generate_values_for_insertion_from_df(df, enrichment, columns)
    assert result == expected_output
