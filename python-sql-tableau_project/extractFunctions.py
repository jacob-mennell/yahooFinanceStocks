#import relevant libs
import pandas as pd
from datetime import datetime
import yfinance as yf
import psycopg2
import sqlalchemy
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

# function to get stock information
def combined_stock_sql_send(stock):
    '''
    Function to pull stock information. Currently pulls historical stock data, major shareholders,
    earnings, quarterly earnings and news.

    Arguments:
        # stock: individual stock in which user wants to return data for.

    Return:
        # returns note in log file to confirm data has been sent to postgres SQL database
    '''

    #start of time function
    start = time.time()

    #path to SQL database stored as environment variable
    postgres_path = os.getenv('postgres_path')
    # dialect+driver://username:password@host:port/database
    engine = sqlalchemy.create_engine(postgres_path)

    # create ticker for the stock
    msft = yf.Ticker(stock)

    try:
        # return historical stock data and send to postgres
        stock_history = msft.history(period="max").reset_index()
        # minor cleaning
        stock_history = stock_history.rename(str.lower, axis='columns')
        stock_history['stock'] = stock
        # send to SQL with SQL Alchemy
        stock_history.to_sql(f'stock_history', engine, if_exists='append', index=False)
        logger.info(f'historical {stock} data sent to sql')
    except Exception as e:
        logging.error("Error getting stock_history data", e)

    try:
        # return major shareholders and send to postgres
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
        # return financials and send to postgres
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
        # return earnings and send to postgres
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
        # return quarterly earnings and send to postgres
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
        # return news and send to postgres
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
def combinedTables(stock_list):
    '''
      Function to pull stock information for multiple stocks. Currently pulls historical stock data, major shareholders,
      earnings, quarterly earnings and news.

      Arguments:
          # companies: list of stocks that we want to return data for

      Return:
          # returns note in log file to confirm data has been sent to postgres SQL database
      '''

    #path to SQL database stored as environment variable
    postgres_path = os.getenv('postgres_path')
    # dialect+driver://username:password@host:port/database
    engine = sqlalchemy.create_engine(postgres_path)

    #clear tables prior to reuse
    #Add update feature in future to prevent batch load following every run
    try:
        blank_sql()
    except Exception as e:
        logging.error("Error clearing the database", e)

    # create master table
    stock_df = pd.DataFrame(columns=['stock'])
    stock_df['stock'] = [x for x in stock_list]
    stock_df.to_sql('stocks_master', engine, if_exists='replace', index=False)

    # get tables for each individual stock
    # [stock_sql_send(item) for item in companies]
    list(map(combined_stock_sql_send, stock_list))
    return logger.info('\nSQL Updated with combined tables')


def blank_sql():
    '''
          Function to clear PostGreSQL database prior to new batch import.

          Arguments:
              # empty

          Return:
              # returns note in log file to confirm data has been cleared in postgres SQL database
          '''

    #path to SQL database stored as environment variable
    postgres_path = os.getenv('postgres_path')
    # dialect+driver://username:password@host:port/database
    engine = sqlalchemy.create_engine(postgres_path)

    # Look at using the below instead or the .drop() method on individual tables
    #sqlalchemy.schema.MetaData.drop_all(bind=None, tables=None, checkfirst=True)¶

    #earnings
    column_names = ["year", "revenue", "earnings", "stock"]
    earnings = pd.DataFrame(columns=column_names)
    earnings.to_sql('earnings', engine, if_exists='replace', index=False)

    #financials
    column_names = ['date', 'Research Development', 'Effect Of Accounting Charges',
       'Income Before Tax', 'Minority Interest', 'Net Income',
       'Selling General Administrative', 'Gross Profit', 'Ebit',
       'Operating Income', 'Other Operating Expenses', 'Interest Expense',
       'Extraordinary Items', 'Non Recurring', 'Other Items',
       'Income Tax Expense', 'Total Revenue', 'Total Operating Expenses',
       'Cost Of Revenue', 'Total Other Income Expense Net',
       'Discontinued Operations', 'Net Income From Continuing Ops',
       'Net Income Applicable To Common Shares', 'stock']
    financials = pd.DataFrame(columns=column_names)
    financials.to_sql('financials', engine, if_exists='replace', index=False)

    #major_share_holders
    column_names = ['percent', 'detail', 'stock']
    major_share_holders = pd.DataFrame(columns=column_names)
    major_share_holders.to_sql('major_share_holders', engine, if_exists='replace', index=False)

    #news
    column_names = ["stock", "uuid", "title", "publisher", "link", "provider_publish_time", "type"]
    news = pd.DataFrame(columns=column_names)
    news.to_sql('news', engine, if_exists='replace', index=False)

    #quarterly_earnings
    column_names = ['quarter', 'revenue', 'earnings', 'stock']
    quarterly_earnings = pd.DataFrame(columns=column_names)
    quarterly_earnings.to_sql('quarterly_earnings', engine, if_exists='replace', index=False)

    #stock_history
    column_names = ["date", "open", "high", "low", "close", "volume", "dividends", 'stock splits', 'stock']
    stock_history = pd.DataFrame(columns=column_names)
    stock_history.to_sql('stock_history', engine, if_exists='replace', index=False)

    #stocks_master
    column_names = ["stock"]
    stocks_master = pd.DataFrame(columns=column_names)
    stocks_master.to_sql('stocks_master', engine, if_exists='replace', index=False)

    return logger.info('PostGreSQL database cleared for import')


def exchange_rate_table(stock_list, period, interval='1d'):
    '''
          Function to create exchange rate table in PostGreSQL database

          Arguments:
              # stock list: list of stocks to return exchange rate to GBP for
              # period of time to return data
              # interval - default set to 1 day

          Return:
              # returns note in log file to confirm data has been sent to postgres SQL database
          '''

    # path to SQL database stored as environment variable
    postgres_path = os.getenv('postgres_path')
    # dialect+driver://username:password@host:port/database
    engine = sqlalchemy.create_engine(postgres_path)

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
                interval=interval
            )
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
                        interval=interval
                    )
                    , columns=['Open', 'Close', 'Low', 'High']
                ).assign(
                    FromCurrency=meta_df['FromCurrency'].values[i],
                    ToCurrency=meta_df['ToCurrency'].values[i]
                )
                currency_df = pd.concat([currency_df, currency_help_df])
            except Exception as e:
                logging.error("Error getting exchange rates", e)
                return

        currency_df = currency_df.reset_index()
        logging.info('Exchange Rates Obtained')

        # split date
        # could average exchange rate by week in year to account for missing vlaues
        currency_df['week_number_of_year'] = currency_df['Date'].dt.week
        currency_df['year'] = currency_df['Date'].dt.year

        # create unique id to merge on
        currency_df['currency_iden'] = (currency_df.Date.apply(
            lambda x: x.strftime("%m%d%Y"))) + (currency_df['FromCurrency'])

        # rename columns
        currency_df.rename(columns={
            'Close': 'currency_close'
        }, inplace=True)

        # merge exchange rates with master dataframe
        exchange_df = pd.merge(df, currency_df[['currency_iden',
                                                'currency_close']],
                               how='right',
                               left_on=['currency_code'],
                               right_on=['FromCurrency'])
        exchange_df.to_sql('stocks_master', engine, if_exists='replace', index=False)

        return logger.info('Exchange rate table created in PostGreSQL database')

################# OLD CODE CAN BE REUSED IF WANT STOCKS IN SEPERATE TABLES #################

# def stock_sql_send(stock):
#     '''
#     Function to pull stock information. Currently pulls historical stock data, major shareholders,
#     earnings, quarterly earnings and news.
#
#     Arguments:
#         # stock: individual stock in which user wnats to return data for.
#
#     Return:
#         # returns strings to confirm data has been sent to postgres SQL database
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

