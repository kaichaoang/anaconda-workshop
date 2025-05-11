import src.constants as const


def setup_ingestion_table_query() -> str:
    query = f"""
        BEGIN TRANSACTION;
        DROP TABLE IF EXISTS "{const.raw_external_funds_table}";
        CREATE TABLE IF NOT EXISTS "{const.raw_external_funds_table}" (
            "FINANCIAL TYPE" TEXT,
            "SYMBOL" TEXT,
            "SECURITY NAME" TEXT,
            "SEDOL" TEXT,
            "ISIN" TEXT,
            "PRICE" REAL,
            "QUANTITY" REAL,
            "REALISED P/L" REAL,
            "MARKET VALUE" REAL,
            "FUND NAME" TEXT,
            "REPORTING DATE" TEXT	
        );
        COMMIT;
    """
    return query

def ingestion_query(values) -> str:
    query = f"""
        BEGIN TRANSACTION;
        INSERT INTO "{const.raw_external_funds_table}" (
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY NAME",
            "SEDOL",
            "ISIN",
            "PRICE",
            "QUANTITY",
            "REALISED P/L",
            "MARKET VALUE",
            "FUND NAME",
            "REPORTING DATE"	
        ) VALUES {values};
        COMMIT;
    """
    return query

def get_equity_price_with_nearest_date_query(date):
    query = f"""
        WITH formatted_dates_equity_ref_prices AS (
            SELECT 
                er.SYMBOL,
                er.COUNTRY,
                er."SECURITY NAME",
                er.SECTOR,
                er.INDUSTRY,
                er.CURRENCY,
                printf('%04d-%02d-%02d',
                    CAST(SUBSTR(ep.DATETIME, INSTR(ep.DATETIME, '/') + INSTR(SUBSTR(ep.DATETIME, INSTR(ep.DATETIME, '/') + 1), '/') + 1) AS INTEGER),  -- Year
                    CAST(SUBSTR(ep.DATETIME, 1, INSTR(ep.DATETIME, '/') - 1) AS INTEGER),  -- Month
                    CAST(SUBSTR(ep.DATETIME, INSTR(ep.DATETIME, '/') + 1, INSTR(SUBSTR(ep.DATETIME, INSTR(ep.DATETIME, '/') + 1), '/') - 1) AS INTEGER)  -- Day
                ) AS FORMATTED_DATE,
                ep.DATETIME,
                ep.PRICE
            FROM 
                equity_reference er
            LEFT JOIN
                equity_prices ep
            ON er.SYMBOL = ep.SYMBOL
        ),
        ranked_dates AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY FORMATTED_DATE DESC) AS rn
            FROM 
                formatted_dates_equity_ref_prices
            WHERE 
                FORMATTED_DATE <= '{date}' 
        )
        SELECT 
            SYMBOL as "REF SYMBOL",
            COUNTRY as "REF COUNTRY",
            "SECURITY NAME" as "REF SECURITY NAME",
            SECTOR as "REF SECTOR",
            INDUSTRY as "REF INDUSTRY",
            CURRENCY as "REF CURRENCY",
            FORMATTED_DATE as "REF DATE",
            PRICE as "REF PRICE"
        FROM 
            ranked_dates
        WHERE 
            rn = 1
    """
    return query


def get_equity_recon_report_with_fund_name_and_reporting_date_query(fund_name: str, reporting_date: str) -> str:
    subquery = get_equity_price_with_nearest_date_query(reporting_date)

    query = f"""
        WITH equity_ref_prices AS (
            {subquery}
        )
        SELECT
            raw_ef."FUND NAME",
            raw_ef."FINANCIAL TYPE",
            raw_ef.SYMBOL,
            raw_ef."PRICE",
            raw_ef."REPORTING DATE",
            ref_prices."REF PRICE",
            ref_prices."REF DATE"
        FROM
        {const.raw_external_funds_table} as raw_ef
        JOIN 
        equity_ref_prices as ref_prices
        ON raw_ef.SYMBOL = ref_prices."REF SYMBOL" 
        WHERE raw_ef."FUND NAME" = '{fund_name}' AND
             raw_ef."FINANCIAL TYPE" = '{const.equity_type}' AND
             raw_ef."REPORTING DATE" = '{reporting_date}' AND
             raw_ef.PRICE != ref_prices."REF PRICE"
    """

    return query

def get_bonds_price_with_nearest_date_query(date: str) -> str:
    query = f"""
        WITH bond_ref_price AS (
            SELECT             
                br."SECURITY NAME",
                br."ISIN",
                br."SEDOL",
                br."COUNTRY",
                br."COUPON",
                br."MATURITY DATE",
                br."COUPON FREQUENCY",
                br."SECTOR",
                br."CURRENCY",
                bp.DATETIME,
                bp."PRICE"
            FROM 
                bond_reference br
            LEFT JOIN
                bond_prices bp
            ON br.ISIN = bp.ISIN
        ),
        ranked_dates AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY "ISIN" ORDER BY DATETIME DESC) AS rn
            FROM 
                bond_ref_price
            WHERE 
                DATETIME <= '{date}' 
        )
        SELECT
            "SECURITY NAME" AS "REF SECURITY NAME",
            "ISIN" AS "REF ISIN",
            "SEDOL" AS "REF SEDOL",
            "COUNTRY" AS "REF COUNTRY",
            "COUPON" AS "REF COUPON",
            "MATURITY DATE" AS "REF MATURITY DATE",
            "COUPON FREQUENCY" AS "REF COUPON FREQUENCY",
            "SECTOR" AS "REF SECTOR",
            "CURRENCY" AS "REF CURRENCY",
            "DATETIME" AS "REF DATE",
            "PRICE" AS "REF PRICE"
        FROM 
            ranked_dates
        WHERE 
            rn = 1
    """
    return query


def get_bonds_recon_report_with_fund_name_and_reporting_date_query(fund_name: str, reporting_date: str) -> str:
    subquery = get_bonds_price_with_nearest_date_query(reporting_date)

    query = f"""
        WITH bond_ref_prices AS (
            {subquery}
        )
        SELECT
            raw_ef."FUND NAME",
            raw_ef."FINANCIAL TYPE",
            raw_ef.SYMBOL,
            raw_ef."PRICE",
            raw_ef."REPORTING DATE",
            ref_prices."REF PRICE",
            ref_prices."REF DATE"
        FROM
            {const.raw_external_funds_table} AS raw_ef
        JOIN 
            bond_ref_prices AS ref_prices
        ON 
            raw_ef.SYMBOL = ref_prices."REF ISIN" 
        WHERE 
            raw_ef."FUND NAME" = '{fund_name}' AND
            raw_ef."FINANCIAL TYPE" = '{const.bond_type}' AND
            raw_ef."REPORTING DATE" = '{reporting_date}' AND
            raw_ef.PRICE != ref_prices."REF PRICE"
    """

    return query

def get_recon_report_query(fund_name: str, reporting_date: str) -> str:
    query = f"""
        SELECT * FROM
        (
            {get_equity_recon_report_with_fund_name_and_reporting_date_query(fund_name, reporting_date)}
        )
        UNION
        SELECT * FROM
        (
            {get_bonds_recon_report_with_fund_name_and_reporting_date_query(fund_name, reporting_date)}
        )
    """
    return query

def get_distinct_fund_name_and_reporting_date_query() -> str:
    query = f"""
        SELECT DISTINCT "FUND NAME", "REPORTING DATE" from {const.raw_external_funds_table}
        ORDER BY "REPORTING DATE" ASC
    """

    return query

def get_equities_date_start_subquery(reporting_date:str):
    query = f"""
         SELECT printf('%04d-%02d-%02d',
                    CAST(SUBSTR(ep.DATETIME, INSTR(ep.DATETIME, '/') + INSTR(SUBSTR(ep.DATETIME, INSTR(ep.DATETIME, '/') + 1), '/') + 1) AS INTEGER),  -- Year
                    CAST(SUBSTR(ep.DATETIME, 1, INSTR(ep.DATETIME, '/') - 1) AS INTEGER),  -- Month
                    CAST(SUBSTR(ep.DATETIME, INSTR(ep.DATETIME, '/') + 1, INSTR(SUBSTR(ep.DATETIME, INSTR(ep.DATETIME, '/') + 1), '/') - 1) AS INTEGER)  -- Day
                ) AS FORMATTED_DATE,
                PRICE
         FROM equity_prices as ep
         WHERE SYMBOL = raw_ef.SYMBOL
         AND strftime('%Y', FORMATTED_DATE) = strftime('%Y', '{reporting_date}') 
         AND strftime('%m', FORMATTED_DATE) = strftime('%m', '{reporting_date}') 
         ORDER BY FORMATTED_DATE ASC 
         LIMIT 1
    """
    return query

def get_bonds_date_start_subquery(reporting_date:str):
    query = f"""
         SELECT DATETIME, PRICE
         FROM bond_prices as bp
         WHERE ISIN = raw_ef.SYMBOL
         AND strftime('%Y', DATETIME) = strftime('%Y', '{reporting_date}') 
         AND strftime('%m', DATETIME) = strftime('%m', '{reporting_date}') 
         ORDER BY DATETIME ASC 
         LIMIT 1

    """
    return query

def get_rate_of_return_query(fund_name: str, reporting_date: str):
    query = f"""
        WITH fund_report_data AS (
            SELECT
                raw_ef."FUND NAME",
                raw_ef."FINANCIAL TYPE",
                raw_ef.SYMBOL,
                raw_ef."QUANTITY",
                raw_ef."REPORTING DATE",
                raw_ef."REALISED P/L",
                raw_ef."MARKET VALUE" AS "END MARKET VALUE",
                CASE 
                    WHEN raw_ef."FINANCIAL TYPE" = 'Equities' THEN (SELECT PRICE FROM ({get_equities_date_start_subquery(reporting_date)}))
                    WHEN raw_ef."FINANCIAL TYPE" = 'Government Bond' THEN (SELECT PRICE FROM ({get_bonds_date_start_subquery(reporting_date)}))
                    WHEN raw_ef."FINANCIAL TYPE" = 'CASH' THEN NULL
                    ELSE NULL
                END AS "START_VALUE",
                CASE 
                    WHEN raw_ef."FINANCIAL TYPE" = 'Equities' THEN (SELECT FORMATTED_DATE FROM ({get_equities_date_start_subquery(reporting_date)}))
                    WHEN raw_ef."FINANCIAL TYPE" = 'Government Bond' THEN (SELECT DATETIME FROM ({get_bonds_date_start_subquery(reporting_date)}))
                    WHEN raw_ef."FINANCIAL TYPE" = 'CASH' THEN NULL
                    ELSE NULL
                END AS "START_DATE"
            FROM
                {const.raw_external_funds_table} AS raw_ef
            WHERE 
                raw_ef."FUND NAME" = '{fund_name}' AND
                raw_ef."REPORTING DATE" = '{reporting_date}'
        ),
        fund_ror_data AS (
            SELECT 
                "FUND NAME",
                "END MARKET VALUE" AS "FUND MV END",
                "QUANTITY" * "START_VALUE" AS "FUND MV START",
                "REALISED P/L",
                "REPORTING DATE"
            FROM fund_report_data
        )
        SELECT 
            "FUND NAME",
            "REPORTING DATE",
            (SUM("FUND MV END") - SUM("FUND MV START") + SUM("REALISED P/L")) / NULLIF(SUM("FUND MV START"), 0) AS "ROR" 
        FROM fund_ror_data
    """
    return query

