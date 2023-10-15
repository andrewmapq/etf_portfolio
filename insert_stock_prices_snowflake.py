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

def get_engine_db():
    snowflake_username = 'andrewmapq'
    snowflake_password = 'Thom@sS@nkara29'
    snowflake_account = 'cpqcihe-to04558'
    snowflake_database = 'ETF_CONSTITUENT_DATA'
    snowflake_schema = 'public'
    snowflake_warehouse = 'compute_wh'

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
    engine = get_engine_db()

    query = "select CONSTITUENT_TICKER from CONSTITUENTS where composite_ticker = 'QQQ' and etfg_date = '2021-11-1' order by WEIGHT desc;"
    stocks = pd.read_sql_query(query, con=engine)
    logging.info(stocks)
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

    # Reset the index of the merged DataFrame
    merged_df.reset_index(inplace=True)

    # Display the merged DataFrame
    print(merged_df)


if __name__ == "__main__":
    main()