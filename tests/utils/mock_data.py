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
        "QUANTITY": [174928.1628875706, 5236681.464162043, np.nan],
        "REALISED P/L": [82343.05125738333, -8411.49517383557, np.nan],
        "MARKET VALUE": [12874712.788525194, 528119325.66074204, 198971692.2443514],
        "FUND NAME": ["Applebead", "Applebead", "Applebead"],
        "REPORTING DATE": ["2023-10-01", "2023-10-01", "2023-10-01"],
    })

def get_mock_data_belaware() -> pd.DataFrame:
    return pd.DataFrame({
        "FINANCIAL TYPE": ["Equities", "Government Bond", "CASH"],
        "SYMBOL": ["XYZ", "JP1234567AB", "USDCURR"],
        "SECURITY NAME": ["Company XYZ", "JAPAN (5 YEAR ISSUE) 0.5 % 12/20/2025 ", "CASH"],
        "SEDOL": [None, "BYYNK80", None],
        "ISIN": [None, None, None],
        "PRICE": [50.0, 102.5, np.nan],
        "QUANTITY": [10000, 2000000, np.nan],
        "REALISED P/L": [5000.0, 2000.0, np.nan],
        "MARKET VALUE": [500000.0, 205000000.0, 0.0],
        "FUND NAME": ["Belaware", "Belaware", "Belaware"],
        "REPORTING DATE": ["2023-10-01", "2023-10-01", "2023-10-01"],
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
    ('2023-10-01','JP1103451GC0',100.70),
    ('2023-10-15','JP1103451GC0',100.85),
    ('2023-10-01','JP1234567AB',102.5),
    ('2023-10-15','JP1234567AB',103.0);
    INSERT INTO "equity_reference" ("SYMBOL","COUNTRY","SECURITY NAME","SECTOR","INDUSTRY","CURRENCY") VALUES
    ('SYY','US','Sysco','Consumer Staples','Food Distributors','USD'),
    ('XYZ','US','Company XYZ','Consumer Staples','Food Distributors','USD');
    INSERT INTO "equity_prices" ("DATETIME","SYMBOL","PRICE") VALUES
    ('10/1/2023','SYY',73.6),
    ('10/15/2023','SYY',75.6),
    ('10/1/2023','XYZ',51.1),
    ('10/15/2023','XYZ',52.2);
    COMMIT;
    """
    return query