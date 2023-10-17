import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote
import yfinance as yf
from pandas_datareader import data as pdr
import logging
from logging_config import configure_logging

configure_logging()
yf.pdr_override()

def change_tickers(df_stocks):
    data_change = {'constituent_ticker': ['FB', 'FISV','MRVL.UW','TCOM.UW'],
            'chg_ticker': ['META', 'FI','MRVL','TCOM']}
    df_change = pd.DataFrame(data_change)

    merged_df = df_stocks.merge(df_change, on='constituent_ticker', how='left')

    df_stocks['constituent_ticker'] = merged_df['chg_ticker'].combine_first(df_stocks['constituent_ticker'])

    logging.info(df_stocks)
    return df_stocks

def get_engine_db(user, pwd, acc, db, schema, wh):
    snowflake_username = user
    snowflake_password = pwd
    snowflake_account = acc
    snowflake_database = db
    snowflake_schema = schema
    snowflake_warehouse = wh

    snowflake_password_encoded = quote(snowflake_password)

    engine = create_engine(
        f'snowflake://{snowflake_username}:{snowflake_password_encoded}@{snowflake_account}.snowflakecomputing.com/{snowflake_database}/{snowflake_schema}',
        connect_args={
            'account': snowflake_account,
            'warehouse': snowflake_warehouse,
        },
        echo=False  # Enable echo for debugging SQL statements (optional)
    )
    logging.info('Connected to snowflake')

    return engine

def main():
    # Get connection string
    engine_constituent = get_engine_db('andrewmapq', 'Thom@sS@nkara29','cpqcihe-to04558','ETF_CONSTITUENT_DATA','public','compute_wh')

    query = "select CONSTITUENT_TICKER from CONSTITUENTS where composite_ticker = 'QQQ' and etfg_date = '2021-11-1' order by WEIGHT desc;"
    stocks = pd.read_sql_query(query, con=engine_constituent)
    logging.info(stocks)
    engine_constituent.dispose()
    stocks = change_tickers(stocks)

    # Create an empty list to store DataFrames
    dfs = []

    for stock in stocks['constituent_ticker'].values:
        logging.info(f'Getting prices for the stock :{stock}')
        # download dataframe
        df_stocks = pdr.get_data_yahoo(stock, start="2023-10-01", end="2023-10-13")
        df_stocks["Ticker"] = stock

        dfs.append(df_stocks)

    # Concatenate all DataFrames into a single DataFrame
    merged_df = pd.concat(dfs)
    merged_df.reset_index(inplace=True)

    engine_etf = get_engine_db('andrewmapq', 'Thom@sS@nkara29','cpqcihe-to04558','ETF_PORTFOLIO','feeder','compute_wh')
    table_name = "stock_prices"

    # Display the merged DataFrame
    merged_df.to_sql(table_name, con=engine_etf, if_exists='replace', index=False)
    engine_etf.dispose()


if __name__ == "__main__":
    main()