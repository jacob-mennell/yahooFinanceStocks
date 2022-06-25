#import relevant libs
import pandas as pd
from datetime import datetime
import yfinance as yf
import psycopg2
import sqlalchemy
import os
import time

# function to get stock information


def combined_stock_sql_send(stock):
    '''
    Function to pull stock information. Currently pulls historical stock data, major shareholders,
    earnings, quarterly earnings and news.

    Arguments:
        # stock: individual stock in which user wnats to return data for.

    Return:
        # returns string to confirm data has been sent to postgres SQL database
    '''

    #start of time function
    start = time.time()

    #path to SQL database stored as environment variable
    postgres_path = os.getenv('postgres_path')
    # dialect+driver://username:password@host:port/database
    engine = sqlalchemy.create_engine(postgres_path)

    # create ticker for the stock
    msft = yf.Ticker(stock)

    # return historical stock data and send to postgres
    stock_history = msft.history(period="max").reset_index()
    # minor cleaning
    stock_history = stock_history.rename(str.lower, axis='columns')
    stock_history['stock'] = stock
    # send to SQL with SQL Alchemy
    stock_history.to_sql(f'stock_history', engine, if_exists='append', index=False)
    print(f'historical {stock} data sent to sql')

    # return major shareholders and send to postgres
    major_share_holders = msft.major_holders
    # minor cleaning
    major_share_holders = major_share_holders.rename(columns={0: "percent", 1: "detail"})
    major_share_holders['stock'] = stock
    # send to SQL with SQL Alchemy
    major_share_holders.to_sql(f'major_share_holders', engine, if_exists='append', index=False)
    print(f'major share holders {stock} data sent to sql')

    # return financials and send to postgres
    stock_financials = msft.financials
    stock_financials = stock_financials.transpose()
    stock_financials = stock_financials.reset_index()
    stock_financials.columns.values[0] = "date"
    stock_financials['stock'] = stock
    # send to SQL with SQL Alchemy
    stock_financials.to_sql(f'financials', engine, if_exists='append', index=False)
    print(f'stock financials {stock} data sent to sql')

    # return earnings and send to postgres
    stock_earnings = msft.earnings.reset_index()
    # minor cleaning
    stock_earnings = stock_earnings.rename(str.lower, axis='columns')
    stock_earnings['stock'] = stock
    # send to SQL with SQL Alchemy
    stock_earnings.to_sql(f'earnings', engine, if_exists='append', index=False)
    print(f'{stock} earnings data sent to sql')

    # return quarterly earnings and send to postgres
    stock_quarterly_earnings = msft.quarterly_earnings.reset_index()
    # minor cleaning
    stock_quarterly_earnings = stock_quarterly_earnings.rename(str.lower, axis='columns')
    stock_quarterly_earnings['stock'] = stock
    # send to SQL with SQL Alchemy
    stock_quarterly_earnings.to_sql(f'quarterly_earnings', engine, if_exists='append', index=False)
    print(f'{stock} quarterly earnings data sent to sql')

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
    print(f'{stock} news sent to sql')

    # end of function
    end = time.time()
    print(f'{stock} extracted in {"{:.2f}".format(end - start)} seconds')

# function to extract data for multiple companies for comparison
def combinedTables(companies):

    #path to SQL database stored as environment variable
    postgres_path = os.getenv('postgres_path')
    # dialect+driver://username:password@host:port/database
    engine = sqlalchemy.create_engine(postgres_path)

    #clear tables prior to reuse
    #Add update feature in future to prevent batch load following every run
    blank_sql()

    # create master table
    stock_df = pd.DataFrame(columns=['stock'])
    stock_df['stock'] = [x for x in companies]
    stock_df.to_sql('stocks_master', engine, if_exists='append', index=False)

    # get tables for each individual stock
    # [stock_sql_send(item) for item in companies]
    list(map(combined_stock_sql_send, companies))
    return print('\nSQL Updated')


def blank_sql():

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

    return print('cleared for import')

################################ OLD CODE CAN BE REUSED IF WANT STOCKS IN SEPERATE TABLES ######################

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