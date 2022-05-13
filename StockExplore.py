#import relevant libs
import pandas as pd
import numpy as np
from datetime import datetime
import yfinance as yf
import sqlalchemy
#import psycopg2
import time
import plotly.express as px
import re
import logging
logger = logging.getLogger(__name__)


class ExploreStocks:

    def __init__(self, stock_list):
        self.stock_list = stock_list

        try:
            stocks_df = yf.download(self.stock_list, group_by='Ticker', period='max')
            stocks_df = stocks_df.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
            stocks_df = stocks_df.reset_index()
        except Exception as e:
            print("Error getting stock data", e)
            return

        print('Initial Stock Information Downloaded')

        # create empty dict
        currency_code = {}

        # loop to extract currency for each ticker using .info method
        for ticker in self.stock_list:
            try:
                tick = yf.Ticker(ticker)
                currency_code[ticker] = tick.info['currency']
            except Exception as e:
                print("Error getting currency symbol", e)
                return

        # make dataframe
        currency_code_df = pd.DataFrame(list(currency_code.items()), columns=['Ticker', 'currency_code'])

        # merge with master dataset
        df = pd.merge(stocks_df, currency_code_df, how='left', left_on=['Ticker'], right_on=['Ticker'])

        df['currency_code'] = df['currency_code'].apply(
            lambda x: x.upper())

        # create unique field to join exchange rates later
        df['currency_id'] = (df['Date'].apply(
            lambda x: x.strftime("%m%d%Y"))) + (df['currency_code'])

        print('Currency Extracted and Merged')

        # function to get exchange rates to GBP
        # Create a Datafame with Yahoo Finance Module
        currencylist = [x.upper() for x in (list(df.currency_code.unique()))]  # choose currencies
        period = (input("Enter a period e.g. 25y y=years: "))
        interval = '1d'

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
                print("Error getting exchange rates", e)
                return

        currency_df = currency_df.reset_index()

        print('Exchange Rates Obtained')

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
        master_df = pd.merge(df, currency_df[['currency_iden', 'currency_close']], how='left', left_on=['currency_id'],
                             right_on=['currency_iden'])

        df_na = master_df.loc[master_df['currency_close'].isna()]

        df_na = df_na.loc[~(df_na['currency_code'].str.contains('GBP', case=False, regex=False, na=False))]

        df_na = df_na.groupby(['currency_code']).agg(currency_code_size=('currency_code', 'size')).reset_index()

        print('The number of values that exchange rate could not be found: \n', df_na)

        na_count = df_na['currency_code'].count() / master_df['currency_code'].count() * 100
        print(f'\n% of NaN values in calculated GBP column : {"{:.2f}".format(na_count)}')

        # We know the value of misisng GBP ot GBP currency is 1 so we can change this manually
        master_df.loc[master_df['currency_code'].isin(['GBP']), 'currency_close'] = 1

        # confirm still numeric data type
        master_df['currency_close'] = pd.to_numeric(master_df['currency_close'], downcast='float', errors='coerce')

        # now we can calculate calculated currency
        master_df['GBP_calculated close'] = master_df['Close'] * master_df['currency_close']

        self.stock_history = master_df.copy()

        print('Data Retrieved - access via the stock_hitory attribute ')

    def plot_stock_price(self):

        # plot the stock prices in GBP over time
        fig = px.line(
            self.stock_history.sort_values(by=['Date'], ascending=[True]).dropna(subset=['GBP_calculated close']),
            x='Date', y='GBP_calculated close', color='Ticker', title='Stock Price Over Time')

        fig.update_layout(xaxis_rangeselector_buttons=list([
            dict(label="1m", count=1, step="month", stepmode="backward"),
            dict(label="6m", count=6, step="month", stepmode="backward"),
            dict(label="YTD", count=1, step="year", stepmode="todate"),
            dict(label="1y", count=1, step="year", stepmode="backward"),
            dict(step="all")
        ]))
        fig.update_layout(xaxis_rangeslider_visible=True)
        fig.update_yaxes(title_text='GBP Calculated Close')
        return fig