import os
import logging
import time
from datetime import datetime, date
import pandas as pd
import yfinance as yf
import sqlalchemy 
from sqlalchemy import create_engine, VARCHAR


def setup_logging():
    """
    Function to set up logging.

    Arguments:
        None

    Return:
        None
    """
    # Set up logging
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)

    # Return error and critical logs to the console
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    console_format = '%(asctime)s | %(levelname)s: %(message)s'
    console.setFormatter(logging.Formatter(console_format))
    logger.addHandler(console)

    # Create a log file to capture all logging
    file_handler = logging.FileHandler('dataExtract.log')
    file_handler.setLevel(logging.INFO)
    file_handler_format = '%(asctime)s | %(levelname)s | %(lineno)d: %(message)s'
    file_handler.setFormatter(logging.Formatter(file_handler_format))
    logger.addHandler(file_handler)
    
    return logger

def setup_sql():
    """
    Function to set up the SQL connection.

    Arguments:
        None

    Return:
        engine: SQLAlchemy engine for database connection
    """
    # Check if the required environment variables are set
    required_env_vars = ['SQL_USERNAME', 'SQL_PASSWORD', 'SQL_SERVER', 'SQL_DATABASE']

    if missing_Vars := [
        env_var for env_var in required_env_vars if env_var not in os.environ
    ]:
        raise ValueError(f"Environment variables '{missing_Vars}' are not set.")

    # Set up database connection
    sql_username = os.environ['SQL_USERNAME']
    sql_password = os.environ['SQL_PASSWORD']
    sql_server = os.environ['SQL_SERVER']
    sql_database = os.environ['SQL_DATABASE']
    driver = 'ODBC Driver 17 for SQL Server'

    odbc_str = f'Driver={driver};Server={sql_server},1433;Database={sql_database};Uid={sql_username};Pwd={sql_password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connect_str = f'mssql+pyodbc:///?odbc_connect={odbc_str}'

    return create_engine(connect_str, fast_executemany=True)

def setup():
    """
    Function to set up the database by calling logging and SQL setup functions.

    Arguments:
        None

    Return:
        None
    """
    # logging setup
    logger = setup_logging()
    
    # sql connection setup
    engine = setup_sql()

    # Return the engine if you need it for other parts of your code
    # Otherwise, you can remove the 'return engine' statement from setup_sql()

    logger.info('Database setup completed.')
    
    return engine


def combined_stock_sql_send(stock):
    """
    Function to pull stock information. Currently pulls historical stock data, major shareholders,
    earnings, quarterly earnings and news.

    Arguments:
        # stock: individual stock in which user wants to return data for.

    Return:
        # returns note in log file to confirm data has been sent to SQL database
    """
    # logging setup
    logger = setup_logging()

    # ensure database and logging set up is complete
    engine = setup()

    # start of time function
    start = time.time()

    # create ticker for the stock
    msft = yf.Ticker(stock)

    try:
        ##################################################
        # return historical stock data and send to sql db
        # stock_history = msft.history(period="max").reset_index()

        # stock_history = stock_history.rename(str.lower, axis='columns')
        # stock_history['stock'] = stock
        # stock_history['date'] = pd.to_datetime(stock_history['date'])

        # print(stock_history.columns)
                
        # # send to SQL with SQL Alchemy
        # stock_history.to_sql(
        #     'stock_history',
        #     engine,
        #     dtype={
        #         'date': datetime,
        #         'open': float,
        #         'high': float,
        #         'low': float,
        #         'close': float,
        #         'volume': float,
        #         'dividends': float,
        #         'stock_splits': float,
        #         'stock': VARCHAR,
        #     },
        #     if_exists='append',
        #     index=False,
        # )
        # logger.info(f'historical {stock} data sent to sql')
        # stock_max_date = str(stock_history.date.max())

        # f = open('last_update.txt', 'w')
        # f.write(f"{stock}_date_max {stock_max_date}")

        # ##################################################
        # # return major shareholders and send to SQL
        major_share_holders = msft.major_holders

        # minor cleaning
        major_share_holders = major_share_holders.rename(columns={0: "percent", 1: "detail"})
        major_share_holders['stock'] = stock

        # send to SQL with SQL Alchemy
        major_share_holders.to_sql(
            'major_share_holders', engine, if_exists='append', index=False
        )
        logger.info(f'major share holders {stock} data sent to sql')

        ##################################################
        # return financials and send to SQL
        stock_financials = msft.financials.transpose().reset_index()
        stock_financials.columns.values[0] = "date"
        stock_financials['stock'] = stock

        # send to SQL with SQL Alchemy
        stock_financials.to_sql(
            'financials', engine, if_exists='append', index=False)
        logger.info(f'stock financials {stock} data sent to sql')

        ##################################################
        # return earnings and send to SQL
        # stock_earnings = msft.earnings.reset_index()

        # stock_earnings = stock_earnings.rename(str.lower, axis='columns')
        # stock_earnings['stock'] = stock

        # # send to SQL with SQL Alchemy
        # stock_earnings.to_sql(
        #     'earnings', engine, if_exists='append', index=False)
        # logger.info(f'{stock} earnings data sent to sql')

        ##################################################
        # return quarterly earnings and send to SQL
        # stock_quarterly_earnings = msft.quarterly_earnings.reset_index()

        # stock_quarterly_earnings = stock_quarterly_earnings.rename(str.lower, axis='columns')
        # stock_quarterly_earnings['stock'] = stock

        # # send to SQL with SQL Alchemy
        # stock_quarterly_earnings.to_sql(
        #     'quarterly_earnings', engine, if_exists='append', index=False)
        # logger.info(f'{stock} quarterly earnings data sent to sql')

        ##################################################
        # return news and send to SQL
        # news_list = msft.news
        # column_names = ["stock", "uuid", "title", "publisher", "link", "provider_publish_time", "type"]
        # news_df = pd.DataFrame(columns=column_names)
        # news_df['uuid'] = [x['uuid'] for x in news_list]
        # news_df['title'] = [x['title'] for x in news_list]
        # news_df['publisher'] = [x['publisher'] for x in news_list]
        # news_df['link'] = [x['link'] for x in news_list]

        # old way of formatting dates
        # dates = [int(x['providerPublishTime']) for x in news_list]
        # func = lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')
        # news_df['providerPublishTime'] = list(map(func, dates))
        # formatting dates
        # dates = [int(x['providerPublishTime']) for x in news_list]
        # news_df['provider_publish_time'] = [datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') for x in dates]

        # # type
        # news_df['type'] = [x['type'] for x in news_list]
        # news_df['stock'] = stock

        # send to SQL with SQL Alchemy
        # news_df.to_sql(
        #     'news', engine, if_exists='append', index=False)
        # logger.info(f'{stock} news sent to sql')

    except Exception as e:

        # Log the exception along with its traceback
        logging.exception("An exception occurred: %s", e)

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
    # logging setup
    logger = setup_logging()
    
    engine = setup_sql()
    
    try:
        blank_sql()
    except Exception as e:
        logging.exception("An exception occurred clearing tables: %s", e)
    
    # create master table
    stock_df = pd.DataFrame(columns=['stock'])
    stock_df['stock'] = list(stock_list)
    stock_df.to_sql('stocks_master', engine, dtype=sqlcol(stock_df), if_exists='append', index=False)

    # get tables for each individual stock
    # [stock_sql_send(item) for item in companies]
    list(map(combined_stock_sql_send, stock_list))
    
    #return logger.info('\nSQL Updated with combined tables')


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
                    return line.split(" ")[-1]
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
    
    # logging setup
    logger = setup_logging()
    
    engine = setup_sql()

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
        
        logging.exception("An exception occurred: %s", e)


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
    # logging setup
    logger = setup_logging()
    
    engine = setup_sql()
    
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
            'FromCurrency': list(currencylist),
            'ToCurrency': ['GBP' for a in currencylist],
            'YahooTickers': [f'{a}GBP=X' for a in currencylist],
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

    date_df.to_sql(
        'date_dimension', engine, if_exists='append', index=False)

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
    exchange_df.to_sql(
        'stocks_master', engine, if_exists='append', index=False)
    
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
    # logging setup
    logger = setup_logging()
    
    engine = setup_sql()
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
            dtypedict[i] = sqlalchemy.types.VARCHAR(
                length=max(dfparam[i].apply(lambda x: len(str(x))))
            )

        if "datetime" in j:
            dtypedict[i] = sqlalchemy.types.DateTime()

        if "float" in j:
            dtypedict[i] = sqlalchemy.types.Float(precision=3, asdecimal=True)

        if "int" in j:
            dtypedict[i] = sqlalchemy.types.INT()

    return dtypedict


if __name__ == "__main__":
    

    # extract data and send to SQL database - specify three airline stocks foe example.

    # IAG.L = International Consolidated Airlines Group, S.A.
    # 0293.HK = Cathay Pacific Airways Ltd
    # AF.PA = Air France-KLM SA

    # send individual tables to sql for each stock
    # individualTables(['IAG.L', '0293.HK', 'AF.PA'])

    # combine tables and send to sql
    stock_list = ['IAG.L', '0293.HK', 'AF.PA']
    combined_tables(stock_list)
    
    # send exchange rate table to sql
    exchange_rate_table(stock_list, period='1y', interval='1d')