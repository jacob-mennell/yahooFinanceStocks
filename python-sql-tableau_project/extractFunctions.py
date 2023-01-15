import pandas as pd
from datetime import datetime
import yfinance as yf
import sqlalchemy
from sqlalchemy import create_engine
import os
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.NOTSET)

# return error and critical logs to console
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
console_format = '%(asctime)s | %(levelname)s: %(message)s'
console.setFormatter(logging.Formatter(console_format))
logger.addHandler(console)

# create log file to capture all logging
file_handler = logging.FileHandler('dataExtract.log')
file_handler.setLevel(logging.INFO)
file_handler_format = '%(asctime)s | %(levelname)s | %(lineno)d: %(message)s'
file_handler.setFormatter(logging.Formatter(file_handler_format))
logger.addHandler(file_handler)

# sets the environment variables as python variables
server = 'yfinance.database.windows.net'
username = os.getenv('sqlusername')
password = os.getenv('sqlpassword')
driver = os.getenv('driver')
#
#  path to SQL database stored as environment variable
#  postgres_path = os.getenv('postgres_path')
#  dialect+driver://username:password@host:port/database
# engine = sqlalchemy.create_engine(postgres_path)

# creates the connection string required to connect to the azure sql database
odbc_str = f'Driver={driver};SERVER=yfinance.database.windows.net; database=stocksdb;Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
connect_str = f'mssql+pyodbc:///?odbc_connect={odbc_str}'
# creates the engine to connect to the database with.
# fast_executemany makes the engine insert multiple rows in each insertstatement and imporves the speed of the code drastically
engine = create_engine(connect_str, fast_executemany=True)


# function to get stock information
def combined_stock_sql_send(stock):
    """
    Function to pull stock information. Currently pulls historical stock data, major shareholders,
    earnings, quarterly earnings and news.

    Arguments:
        # stock: individual stock in which user wants to return data for.

    Return:
        # returns note in log file to confirm data has been sent to SQL database
    """

    # start of time function
    start = time.time()

    # create ticker for the stock
    msft = yf.Ticker(stock)

    try:
        # return historical stock data and send to sql db
        stock_history = msft.history(period="max").reset_index()
        # minor cleaning
        stock_history = stock_history.rename(str.lower, axis='columns')
        stock_history['stock'] = stock
        stock_history['date'] = pd.to_datetime(stock_history['date'])

        # send to SQL with SQL Alchemy
        stock_history.to_sql(f'stock_history', engine,
                             # dtype=
                             # {'date': datetime,
                             #  'open': float,
                             #  'high': float,
                             #  'low': float,
                             #  'close': float,
                             #  'volume': float,
                             #  'dividends': float,
                             #  'stock_splits': float,
                             #  #   'stock': VARCHAR
                             #  }
                             if_exists='append', index=False)
        logger.info(f'historical {stock} data sent to sql')
        stock_max_date = str(stock_history.date.max())
        # set new dates to limit size of future uploads
        f = open('last_update.txt', 'w')
        f.write(f"{stock}_date_max {stock_max_date}")
    except Exception as e:
        logging.error("Error getting stock_history data", e)

    try:
        # return major shareholders and send to SQL
        major_share_holders = msft.major_holders
        # minor cleaning
        major_share_holders = major_share_holders.rename(columns={0: "percent", 1: "detail"})
        major_share_holders['stock'] = stock
        # send to SQL with SQL Alchemy
        major_share_holders.to_sql(f'major_share_holders', engine, if_exists='append', index=False)
        logger.info(f'major share holders {stock} data sent to sql')
    except Exception as e:
        logging.error("Error getting major_share_holders data", e)

    try:
        # return financials and send to SQL
        stock_financials = msft.financials
        stock_financials = stock_financials.transpose()
        stock_financials = stock_financials.reset_index()
        stock_financials.columns.values[0] = "date"
        stock_financials['stock'] = stock
        # send to SQL with SQL Alchemy
        stock_financials.to_sql(f'financials', engine, if_exists='append', index=False)
        logger.info(f'stock financials {stock} data sent to sql')
    except Exception as e:
        logging.error("Error getting stock_financials data", e)

    try:
        # return earnings and send to SQL
        stock_earnings = msft.earnings.reset_index()
        # minor cleaning
        stock_earnings = stock_earnings.rename(str.lower, axis='columns')
        stock_earnings['stock'] = stock
        # send to SQL with SQL Alchemy
        stock_earnings.to_sql(f'earnings', engine, if_exists='append', index=False)
        logger.info(f'{stock} earnings data sent to sql')
    except Exception as e:
        logging.error("Error getting earnings data", e)

    try:
        # return quarterly earnings and send to SQL
        stock_quarterly_earnings = msft.quarterly_earnings.reset_index()
        # minor cleaning
        stock_quarterly_earnings = stock_quarterly_earnings.rename(str.lower, axis='columns')
        stock_quarterly_earnings['stock'] = stock
        # send to SQL with SQL Alchemy
        stock_quarterly_earnings.to_sql(f'quarterly_earnings', engine, if_exists='append', index=False)
        logger.info(f'{stock} quarterly earnings data sent to sql')
    except Exception as e:
        logging.error("Error getting quarterly earnings data", e)

    try:
        # return news and send to SQL
        news_list = msft.news
        column_names = ["stock", "uuid", "title", "publisher", "link", "provider_publish_time", "type"]
        news_df = pd.DataFrame(columns=column_names)
        news_df['uuid'] = [x['uuid'] for x in news_list]
        news_df['title'] = [x['title'] for x in news_list]
        news_df['publisher'] = [x['publisher'] for x in news_list]
        news_df['link'] = [x['link'] for x in news_list]
        # old way of formatting dates
        # dates = [int(x['providerPublishTime']) for x in news_list]
        # func = lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')
        # news_df['providerPublishTime'] = list(map(func, dates))
        # formatting dates
        dates = [int(x['providerPublishTime']) for x in news_list]
        news_df['provider_publish_time'] = [datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') for x in dates]
        # type
        news_df['type'] = [x['type'] for x in news_list]
        news_df['stock'] = stock
        # send to SQL with SQL Alchemy
        news_df.to_sql(f'news', engine, if_exists='append', index=False)
        logger.info(f'{stock} news sent to sql')
    except Exception as e:
        logging.error("Error getting news data", e)

    # end of function
    end = time.time()
    logger.info(f'{stock} extracted in {"{:.2f}".format(end - start)} seconds')


# function to extract data for multiple companies for comparison
def combined_tables(stock_list):
    """
    Function to pull stock information for multiple stocks. Currently pulls historical stock data, major shareholders,
    earnings, quarterly earnings and news.

    Arguments:
        # companies: list of stocks that we want to return data for

    Return:
        # returns note in log file to confirm data has been sent to SQL database
    """
    try:
        blank_sql()
    except Exception as e:
        logging.error("Error clearing tables", e)

    # create master table
    stock_df = pd.DataFrame(columns=['stock'])
    stock_df['stock'] = [x for x in stock_list]
    stock_df.to_sql('stocks_master', engine, dtype=sqlcol(stock_df), if_exists='append', index=False)

    # get tables for each individual stock
    # [stock_sql_send(item) for item in companies]
    list(map(combined_stock_sql_send, stock_list))
    return logger.info('\nSQL Updated with combined tables')


def get_last_update_date(stock):
    """
    Function to retrieve the last update date for a stock
    Arguments:
        stock: individual stock to retrieve the last update date for
    Returns:
        last_update_date: the last update date for the specified stock
    """
    try:
        with open('last_update.txt', 'r') as f:
            lines = f.readlines()
            for line in lines:
                if f"{stock}_date_max" in line:
                    last_update_date = line.split(" ")[-1]
                    return last_update_date
    except FileNotFoundError:
        return None


def stock_history_updater(stock):
    """
    Function to pull stock information. Pulls historical data from date of last update.

    Arguments:
        # stock: individual stock in which user wants to return data for.

    Return:
        # returns note in log file to confirm data has been sent to SQL database
    """

    # start of time function
    start = time.time()

    # create ticker for the stock
    msft = yf.Ticker(stock)

    # replaced with get_last_update_date(stock) func
    # create dicts of dates from text file for date filtering prior to upload
    # date_dict = {}
    # with open("last_update.txt") as f:
    #     for line in f:
    #         k, v = line.split(' ', 1)
    #         v = v[:-1]
    #         date_dict[k] = v
    # f.close

    try:
        # return historical stock data and send to sql db
        today_date = date.today()
        stock_history = msft.history(start=get_last_update_date(stock), end=today_date, interval="1d").reset_index()
        # minor cleaning
        stock_history = stock_history.rename(str.lower, axis='columns')
        stock_history['stock'] = stock
        stock_history['date'] = pd.to_datetime(stock_history['date'])
        # send to SQL with SQL Alchemy
        stock_history.to_sql(f'stock_history', engine, if_exists='append', index=False)
        logger.info(f'historical {stock} data sent to sql')
        stock_max_date = str(stock_history.date.max())
        # set new dates to limit size of future uploads
        f = open('last_update.txt', 'w')
        f.write(f"{stock}_date_max {stock_max_date}")
    except Exception as e:
        logging.error("Error getting stock_history data", e)


def exchange_rate_table(stock_list, period, interval='1d'):
    """
    Function to create exchange rate table and date dimension table in SQL database

    Arguments:
        # stock list: list of stocks to return exchange rate to GBP for
        # period of time to return data
        # interval - default set to 1 day. Needs to be one day for date dimension table.

    Return:
        # returns note in log file to confirm data has been sent to SQL database
    """

    currency_code = {}
    # loop to extract currency for each ticker using .info method
    for ticker in stock_list:
        try:
            tick = yf.Ticker(ticker)
            currency_code[ticker] = tick.info['currency']
        except Exception as e:
            logging.error("Error getting currency symbol", e)

    # make dataframe
    df = pd.DataFrame(list(currency_code.items()), columns=['Ticker', 'currency_code'])
    df['currency_code'] = df['currency_code'].apply(
        lambda x: x.upper())

    # Create a Datafame with Yahoo Finance Module
    currencylist = [x.upper() for x in (list(df.currency_code.unique()))]  # choose currencies
    meta_df = pd.DataFrame(
        {
            'FromCurrency': [a for a in currencylist],
            'ToCurrency': ['GBP' for a in currencylist],
            'YahooTickers': [f'{a}GBP=X' for a in currencylist]
        }
    )

    currency_df = pd.DataFrame(
        yf.download(
            tickers=meta_df['YahooTickers'].values[0],
            period=period,
            interval=interval)
        , columns=['Open', 'Close', 'Low', 'High']
    ).assign(
        FromCurrency=meta_df['FromCurrency'].values[0],
        ToCurrency=meta_df['ToCurrency'].values[0]
    )
    for i in range(1, len(meta_df)):
        try:
            currency_help_df = pd.DataFrame(
                yf.download(
                    tickers=meta_df['YahooTickers'].values[i],
                    period=period,
                    interval=interval)
                , columns=['Open', 'Close', 'Low', 'High']).assign(
                FromCurrency=meta_df['FromCurrency'].values[i],
                ToCurrency=meta_df['ToCurrency'].values[i])

            currency_df = pd.concat([currency_df, currency_help_df])
            currency_df = currency_df.reset_index()
            logging.info('Exchange Rates Obtained')

        except Exception as e:
            logging.error("Error getting exchange rates", e)

    # use exchange rates table with daily increments to crate date dimension table
    date_df = pd.DataFrame()
    date_df['date_key'] = currency_df['Date'].dt.strftime('%m%d%Y')
    date_df['year'] = currency_df['Date'].dt.year
    date_df['quarter'] = currency_df['Date'].dt.quarter
    date_df['month'] = currency_df['Date'].dt.month
    # date_df['week_number_of_year'] = currency_df['Date'].dt.week
    date_df.to_sql('date_dimension', engine, if_exists='append', index=False)

    # create unique id
    currency_df['exchange_id'] = (currency_df.Date.apply(
        lambda x: x.strftime("%m%d%Y"))) + (currency_df['FromCurrency'])
    currency_df['currency_date_key'] = (currency_df.Date.apply(
        lambda x: x.strftime("%m%d%Y")))

    # rename columns
    currency_df.rename(columns={
        'Close': 'currency_close'}
        , inplace=True)

    # merge exchange rates with master dataframe
    exchange_df = pd.merge(df, currency_df,
                           how='right',
                           left_on=['currency_code'],
                           right_on=['FromCurrency'])
    exchange_df.to_sql('stocks_master', engine, if_exists='append', index=False)
    return logger.info('Exchange rate table created in SQL database')


def blank_sql():
    """
    Function to clear database prior to new batch import.
    To be replaced with drop() or drop_all() method.

    Arguments:
        # empty

    Return:
        # returns note in log file to confirm data has been cleared in postgres SQL database
    """

    connection = engine.raw_connection()
    cursor = connection.cursor()
    # add in a sql statement that drops all tables.
    table_list = ["earnings",
                  "financials",
                  "major_share_holders",
                  "news",
                  "quarterly_earnings",
                  "stock_history",
                  "stocks_master"]

    for table in table_list:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
        connection.commit()
        logger.info(f'{table} dropped from database')
    cursor.close()


def sqlcol(dfparam):
    dtypedict = {}
    dtypes = [str(x) for x in dfparam.dtypes.values]
    for i, j in zip(dfparam.columns, dtypes):
        if "object" in j:
            dtypedict.update({i: sqlalchemy.types.VARCHAR(length=max(dfparam[i].apply(lambda x: len(str(x)))))})

        if "datetime" in j:
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in j:
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

        if "int" in j:
            dtypedict.update({i: sqlalchemy.types.INT()})

    return dtypedict
# -------------------- OLD CODE CAN BE REUSED IF NEEDED STOCKS IN SEPERATE TABLES --------------------

# def stock_sql_send(stock):
#     '''
#     Function to pull stock information. Currently pulls historical stock data, major shareholders,
#     earnings, quarterly earnings and news.
#
#     Arguments:
#         # stock: individual stock in which user wnats to return data for.
#
#     Return:
#         # returns strings to confirm data has been sent to  SQL database
#     '''
#
#     #start of time function
#     start = time.time()
#
#     #path to SQL database stored as environment variable
#     postgres_path = os.getenv('postgres_path')
#     # dialect+driver://username:password@host:port/database
#     engine = sqlalchemy.create_engine(postgres_path)
#
#     # create ticker for the stock
#     msft = yf.Ticker(stock)
#
#     # return historical stock data and send to postgres
#     stock_history = msft.history(period="max").reset_index()
#     # minor cleaning
#     stock_history = stock_history.rename(str.lower, axis='columns')
#     stock_history['stock'] = stock
#     # send to SQL with SQL Alchemy
#     stock_history.to_sql(f'{stock}_history', engine, if_exists='replace', index=False)
#     print(f'historical {stock} data sent to sql')
#
#     # return major shareholders and send to postgres
#     major_share_holders = msft.major_holders
#     # minor cleaning
#     major_share_holders = major_share_holders.rename(columns={0: "percent", 1: "detail"})
#     major_share_holders['stock'] = stock
#     # send to SQL with SQL Alchemy
#     major_share_holders.to_sql(f'{stock}_major_share_holders', engine, if_exists='replace', index=False)
#     print(f'major share holders {stock} data sent to sql')
#
#     # return financials and send to postgres
#     stock_financials = msft.financials
#     stock_financials = stock_financials.transpose()
#     stock_financials = stock_financials.reset_index()
#     stock_financials.columns.values[0] = "date"
#     stock_financials['stock'] = stock
#     # send to SQL with SQL Alchemy
#     stock_financials.to_sql(f'{stock}_financials', engine, if_exists='replace', index=False)
#     print(f'stock financials {stock} data sent to sql')
#
#     # return earnings and send to postgres
#     stock_earnings = msft.earnings.reset_index()
#     # minor cleaning
#     stock_earnings = stock_earnings.rename(str.lower, axis='columns')
#     stock_earnings['stock'] = stock
#     # send to SQL with SQL Alchemy
#     stock_earnings.to_sql(f'{stock}_earnings', engine, if_exists='replace', index=False)
#     print(f'{stock} earnings data sent to sql')
#
#     # return quarterly earnings and send to postgres
#     stock_quarterly_earnings = msft.quarterly_earnings.reset_index()
#     # minor cleaning
#     stock_quarterly_earnings = stock_quarterly_earnings.rename(str.lower, axis='columns')
#     stock_quarterly_earnings['stock'] = stock
#     # send to SQL with SQL Alchemy
#     stock_quarterly_earnings.to_sql(f'{stock}_quarterly_earnings', engine, if_exists='replace', index=False)
#     print(f'{stock} quarterly earnings data sent to sql')
#
#     # return news and send to postgres
#     news_list = msft.news
#     column_names = ["stock", "uuid", "title", "publisher", "link", "provider_publish_time", "type"]
#     news_df = pd.DataFrame(columns=column_names)
#     news_df['uuid'] = [x['uuid'] for x in news_list]
#     news_df['title'] = [x['title'] for x in news_list]
#     news_df['publisher'] = [x['publisher'] for x in news_list]
#     news_df['link'] = [x['link'] for x in news_list]
#     # old way of formatting dates
#     # dates = [int(x['providerPublishTime']) for x in news_list]
#     # func = lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')
#     # news_df['providerPublishTime'] = list(map(func, dates))
#     # formatting dates
#     dates = [int(x['providerPublishTime']) for x in news_list]
#     news_df['provider_publish_time'] = [datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') for x in dates]
#     # type
#     news_df['type'] = [x['type'] for x in news_list]
#     news_df['stock'] = stock
#     # send to SQL with SQL Alchemy
#     news_df.to_sql(f'{stock}_news', engine, if_exists='replace', index=False)
#     print(f'{stock} news sent to sql')
#
#     # end of function
#     end = time.time()
#     print(f'{stock} extracted in {"{:.2f}".format(end - start)} seconds')
#
#
# # function to extract data for multiple companies for comparison
# def individualTables(companies):
#     # create master table
#     stock_df = pd.DataFrame(columns=['stock'])
#     stock_df['stock'] = [x for x in companies]
#
#     #path to SQL database stored as environment variable
#     postgres_path = os.getenv('postgres_path')
#     # dialect+driver://username:password@host:port/database
#     engine = sqlalchemy.create_engine(postgres_path)
#     stock_df.to_sql('stocks_master', engine, if_exists='replace', index=False)
#
#     # get tables for each individual stock
#     # [stock_sql_send(item) for item in companies]
#     list(map(stock_sql_send, companies))
#     return print('\nSQL Updated')
