import numpy as np
import pandas as pd


def get_mock_data_applebead() -> pd.DataFrame:
    return pd.DataFrame({
        "FINANCIAL TYPE": ["Equities", "Government Bond", "CASH"],
        "SYMBOL": ["SYY", "JP1103451GC0", "USDCURR"],
        "SECURITY NAME": ["Sysco", "JAPAN (10 YEAR ISSUE) 0.1 % 12/20/2026 ", "CASH"],
        "SEDOL": [None, "BYYNK79", None],
        "ISIN": [None, None, None],
        "PRICE": [73.6, 100.85, np.nan],
        "QUANTITY": [100, 100, np.nan],
        "REALISED P/L": [5000.0, 2000.0, np.nan],
        "MARKET VALUE": [50000.0, 20000, 0.0],
        "FUND NAME": ["Applebead", "Applebead", "Applebead"],
        "REPORTING DATE": ["2023-10-31", "2023-10-31", "2023-10-31"],
    })

def get_mock_data_belaware() -> pd.DataFrame:
    return pd.DataFrame({
        "FINANCIAL TYPE": ["Equities", "Government Bond", "CASH"],
        "SYMBOL": ["XYZ", "JP1234567AB", "USDCURR"],
        "SECURITY NAME": ["Company XYZ", "JAPAN (5 YEAR ISSUE) 0.5 % 12/20/2025 ", "CASH"],
        "SEDOL": [None, "BYYNK80", None],
        "ISIN": [None, None, None],
        "PRICE": [50.0, 102.5, np.nan],
        "QUANTITY": [100, 100, np.nan],
        "REALISED P/L": [4000.0, 2100.0, np.nan],
        "MARKET VALUE": [40000.0, 21000, 0.0],
        "FUND NAME": ["Belaware", "Belaware", "Belaware"],
        "REPORTING DATE": ["2023-10-31", "2023-10-31", "2023-10-31"],
    })

def get_mock_master_sql() -> str:
    query = f"""
    BEGIN TRANSACTION;
    DROP TABLE IF EXISTS "bond_reference";
    CREATE TABLE IF NOT EXISTS "bond_reference" (
        "SECURITY NAME" TEXT,
        "ISIN" TEXT,
        "SEDOL" TEXT,
        "COUNTRY" TEXT,
        "COUPON" REAL,
        "MATURITY DATE" TEXT,
        "COUPON FREQUENCY" TEXT,
        "SECTOR" TEXT,
        "CURRENCY" TEXT
    );
    DROP TABLE IF EXISTS "bond_prices";
    CREATE TABLE IF NOT EXISTS "bond_prices" (
        "DATETIME" TEXT,
        "ISIN" TEXT,
        "PRICE" REAL
    );
    DROP TABLE IF EXISTS "equity_reference";
    CREATE TABLE IF NOT EXISTS "equity_reference" (
        "SYMBOL" TEXT,
        "COUNTRY" TEXT,
        "SECURITY NAME" TEXT,
        "SECTOR" TEXT,
        "INDUSTRY" TEXT,
        "CURRENCY" TEXT
    );
    DROP TABLE IF EXISTS "equity_prices";
    CREATE TABLE IF NOT EXISTS "equity_prices" (
        "DATETIME" TEXT,
        "SYMBOL" TEXT,
        "PRICE" REAL
    );
    INSERT INTO "bond_reference" ("SECURITY NAME","ISIN","SEDOL","COUNTRY","COUPON","MATURITY DATE","COUPON FREQUENCY","SECTOR","CURRENCY") VALUES
    ('JAPAN (10 YEAR ISSUE) 0.1 % 12/20/2026 ','JP1103451GC0','BYYNK79','JP',0.1,'20/12/2026','Levery 6 month','Treasury','USD'),
    ('JAPAN (5 YEAR ISSUE) 0.5 % 12/20/2025 ','JP1234567AB','BYYNK80','JP',0.1,'20/12/2026','Levery 6 month','Treasury','USD');
    INSERT INTO "bond_prices" ("DATETIME","ISIN","PRICE") VALUES
    ('2023-10-02','JP1103451GC0',100.70),
    ('2023-10-30','JP1103451GC0',100.85),
    ('2023-10-02','JP1234567AB',102.5),
    ('2023-10-30','JP1234567AB',103.0);
    INSERT INTO "equity_reference" ("SYMBOL","COUNTRY","SECURITY NAME","SECTOR","INDUSTRY","CURRENCY") VALUES
    ('SYY','US','Sysco','Consumer Staples','Food Distributors','USD'),
    ('XYZ','US','Company XYZ','Consumer Staples','Food Distributors','USD');
    INSERT INTO "equity_prices" ("DATETIME","SYMBOL","PRICE") VALUES
    ('10/2/2023','SYY',73.6),
    ('10/30/2023','SYY',75.6),
    ('10/2/2023','XYZ',51.1),
    ('10/30/2023','XYZ',50.0);
    COMMIT;
    """
    return query
