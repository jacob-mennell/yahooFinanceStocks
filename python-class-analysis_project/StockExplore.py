import pandas as pd
import numpy as np
from datetime import datetime
import yfinance as yf
import sqlalchemy
import time
import plotly.express as px
import logging
from prophet import Prophet
from prophet.diagnostics import cross_validation
from prophet.diagnostics import performance_metrics
from prophet.plot import plot_plotly, plot_components_plotly, add_changepoints_to_plot
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.metrics import mean_absolute_error
from dask.distributed import Client
import itertools


class ExploreStocks:

    def __init__(self, stock_list, period):
        self.stock_list = stock_list
        self.period = period

        # initialise log file
        logger = logging.getLogger()
        logger.setLevel(logging.NOTSET)

        # return error and critical logs to console
        console = logging.StreamHandler()
        console.setLevel(logging.ERROR)
        console_format = '%(asctime)s | %(levelname)s: %(message)s'
        console.setFormatter(logging.Formatter(console_format))
        logger.addHandler(console)

        # create log file to capture all logging
        file_handler = logging.FileHandler('ExploreStocks.log')
        file_handler.setLevel(logging.INFO)
        file_handler_format = '%(asctime)s | %(levelname)s | %(lineno)d: %(message)s'
        file_handler.setFormatter(logging.Formatter(file_handler_format))
        logger.addHandler(file_handler)

        # download stock initial stock info
        try:
            stocks_df = yf.download(self.stock_list,
                                    group_by='Ticker',
                                    period='max')
            stocks_df = stocks_df.stack(level=0).rename_axis(['Date',
                                                              'Ticker']).reset_index(level=1)
            stocks_df = stocks_df.reset_index()
        except Exception as e:
            logging.error("Error getting stock data", e)
            return

        logging.info('Initial Stock Information Downloaded')

        # next get currency code for each stock
        currency_code = {}
        # loop to extract currency for each ticker using .info method
        for ticker in self.stock_list:
            try:
                tick = yf.Ticker(ticker)
                currency_code[ticker] = tick.info['currency']
            except Exception as e:
                logging.error("Error getting currency symbol", e)
                return
        # make dataframe
        currency_code_df = pd.DataFrame(list(currency_code.items()), columns=['Ticker',
                                                                              'currency_code'])
        # merge with master dataset
        df = pd.merge(stocks_df,
                      currency_code_df,
                      how='left',
                      left_on=['Ticker'],
                      right_on=['Ticker'])

        df['currency_code'] = df['currency_code'].apply(
            lambda x: x.upper())
        # create unique field to join exchange rates later
        df['currency_id'] = (df['Date'].apply(
            lambda x: x.strftime("%m%d%Y"))) + (df['currency_code'])

        logging.info('Currency Extracted and Merged')

        # function to get exchange rates to GBP table

        # Create a Datafame with Yahoo Finance Module
        currencylist = [x.upper() for x in (list(df.currency_code.unique()))]  # choose currencies
        interval = '1d'
        # period = (input("Enter a period e.g. 25y y=years: "))

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

        self.currency_df = currency_df

        # merge exchange rates with master dataframe
        master_df = pd.merge(df, currency_df[['currency_iden',
                                              'currency_close']],
                             how='left',
                             left_on=['currency_id'],
                             right_on=['currency_iden'])

        # Eliminate weekend from future dataframe
        master_df['day'] = master_df['Date'].dt.weekday
        master_df = master_df[master_df['day'] <= 4]

        # look for na values
        df_na = master_df.loc[master_df['currency_close'].isna()]
        df_na = df_na.loc[~(df_na['currency_code'].str.contains('GBP', case=False, regex=False, na=False))]
        df_na = df_na.groupby(['currency_code']).agg(currency_code_size=('currency_code', 'size')).reset_index()
        na_count = df_na['currency_code'].count() / master_df['currency_code'].count() * 100
        print(f'\n% of NaN values in calculated GBP column : {"{:.2f}".format(na_count)}')

        # We know the value of missing GBP ot GBP currency is 1, so we can change this manually
        master_df.loc[master_df['currency_code'].isin(['GBP']), 'currency_close'] = 1
        # confirm still numeric data type
        master_df['currency_close'] = pd.to_numeric(master_df['currency_close'], downcast='float', errors='coerce')
        # now we can calculate calculated currency
        master_df['GBP_calculated close'] = master_df['Close'] * master_df['currency_close']

        self.stock_history = master_df.copy()

        logging.info('Data Retrieved - dataframe with exchange rates initialised')
        print('Data Retrieved - access via the stock_history attribute ')

    def return_df(self):
        """ Returns: dataframe - also return by calling self.stock_history """
        return self.stock_history

    def plot_stock_price(self, log=False, **kwargs):
        """
        Args:
            log:
            **kwargs:

        Returns: plotly plot of the stock price for each stock over time
         """

        fig = px.line(
            self.stock_history.sort_values(by=['Date'],
            ascending=[True]).dropna(subset=['GBP_calculated close']),
            x='Date',
            y='GBP_calculated close',
            color='Ticker',
            title='Stock Price Over Time',**kwargs )

        fig.update_layout(xaxis_rangeselector_buttons=list([
            dict(label="1m", count=1, step="month", stepmode="backward"),
            dict(label="6m", count=6, step="month", stepmode="backward"),
            dict(label="YTD", count=1, step="year", stepmode="todate"),
            dict(label="1y", count=1, step="year", stepmode="backward"),
            dict(step="all")
        ]))
        fig.update_layout(xaxis_rangeslider_visible=True)
        fig.update_yaxes(title_text='GBP Calculated Close')

        if log:
            fig.update_yaxes(type='log', tickformat='.1e')
        return fig

    def plot_trade_volume(self,**kwargs):
        """
        Args:
            **kwargs:

        Returns: Plot of the volume traded for each stock over time """

        fig = px.line(self.stock_history.sort_values(by=['Date'],ascending=[True]),
                      x='Date',
                      y='Volume',
                      color='Ticker',
                      facet_col='Ticker',
                      title='Volume Traded Over Time'
                      ,**kwargs)
        return fig

    def plot_volatility(self,**kwargs):
        """
        Args:
            **kwargs:

        Returns: plot of the volatility of each stock (daily close % change)
         """

        # compute daily percent change in closing price
        self.stock_history['returns'] = self.stock_history.groupby("Ticker")['Close'].pct_change()

        # get title
        title = (' '.join([str(item) for item in self.stock_list])) + ' Daily Volatility Comparison'

        # plot histogram
        fig = px.histogram(self.stock_history.dropna(subset=['returns']),
                           x='returns',
                           title=title,
                           color='Ticker',
                           **kwargs,
                           nbins=200)
        fig.update_yaxes(title_text='Count')
        fig.update_xaxes(title_text='Return Bins')
        return fig

    def plot_cumulative_returns(self,**kwargs):
        """
        Args:
            **kwargs:

        Returns: plot of the cumulative return of each stock over time
        """

        cum_returns = self.stock_history[['Date', 'Close', 'Ticker']]
        cum_returns = pd.pivot_table(cum_returns, columns=['Ticker'], index=['Date'])

        daily_pct_change = cum_returns.pct_change()
        daily_pct_change.fillna(0, inplace=True)
        cumprod_daily_pct_change = (1 + daily_pct_change).cumprod()

        cumprod_daily_pct_change.columns = ["_".join([str(index) for index in multi_index]) for multi_index in
                                            cumprod_daily_pct_change.columns.ravel()]
        cumprod_daily_pct_change = cumprod_daily_pct_change.reset_index()

        # get title
        title = (' '.join([str(item) for item in self.stock_list])) + ' Cumulative Returns'

        fig = px.line(cumprod_daily_pct_change.sort_values(by=['Date'],ascending=[True]),
                      x='Date',
                      y=['Close_0293.HK', 'Close_AF.PA', 'Close_IAG.L'],
                      title=title,
                      **kwargs)

        fig.update_yaxes(title_text='Cumulative Returns')
        fig.update_layout(xaxis_rangeslider_visible=True)
        fig.update_layout(legend_title_text='Ticker')
        return fig

    def plot_rolling_average(self,**kwargs):
        """
        Args:
            **kwargs:

        Returns: plot of the rolling average of each stock over time
        """

        # compute several rolling means
        gbp_df = self.stock_history.copy()

        gbp_df['MA50'] = gbp_df.groupby('Ticker')['GBP_calculated close'].transform(lambda x: x.rolling(50, 25).mean())
        gbp_df['MA200'] = gbp_df.groupby('Ticker')['GBP_calculated close'].transform(
            lambda x: x.rolling(200, 100).mean())
        gbp_df['MA365'] = gbp_df.groupby('Ticker')['GBP_calculated close'].transform(
            lambda x: x.rolling(365, 182).mean())
        gbp_df['MA1000'] = gbp_df.groupby('Ticker')['GBP_calculated close'].transform(
            lambda x: x.rolling(1000, 500).mean())

        # Visualise the rolling mean over time
        fig = px.line(gbp_df.sort_values(by=['Date'], ascending=[True]).dropna(
            subset=['MA1000', 'GBP_calculated close', 'MA200']),
            x='Date',
            y=['MA1000', 'MA200', 'GBP_calculated close'],
            facet_row='Ticker',
            title='Rolling Mean Stock Price Over Time', **kwargs)

        # add custom y-axis for each facet
        #         for k in fig.layout:
        #             if re.search('yaxis[1-9]+', k):
        #                 fig.layout[k].update(matches=None)
        fig.update_yaxes(matches=None)

        # Add Date slider
        fig.update_layout(xaxis_rangeselector_buttons=list([
            dict(label="1m", count=1, step="month", stepmode="backward"),
            dict(label="6m", count=6, step="month", stepmode="backward"),
            dict(label="YTD", count=1, step="year", stepmode="todate"),
            dict(label="1y", count=1, step="year", stepmode="backward"),
            dict(step="all")
        ]))
        fig.update_layout(xaxis_rangeslider_visible=True)
        fig.update_layout(legend_title_text='Average')
        fig.update_yaxes(title_text=f'Stock Value')

        return fig

    def plot_future_trend(self, stock, start_date='2021-05-01', periods=90, country_name='US',
                          changepoints=True, trend=True, cap=1000, floor=0, growth='logistic',
                          interval_width=0.95,**kwargs):
        """
        Function to predict the future trend of a stock.

        Args:
            stock:
            start_date:
            periods:
            country_name:
            changepoints:
            trend:
            cap:
            floor:
            growth:
            interval_width:
            **kwargs:

        Returns: plotly figure of future trend.
        """

        post_date_df = self.stock_history.loc[~(self.stock_history['Date'] <= start_date)]
        predict_df = post_date_df.loc[post_date_df['Ticker'].isin([stock])]

        # rename columns to fit model
        df = predict_df.rename(columns={'Date': 'ds', 'Close': 'y'})
        df = df[['ds', 'y']]
        df['cap'] = cap
        df['floor'] = floor

        m = Prophet(yearly_seasonality=True, growth=growth, interval_width=interval_width)

        # get currency code for stock
        currency_code = predict_df['currency_code'].values[0]

        # HOLIDAYS - default is US
        if currency_code == 'GBP':
            m.add_country_holidays(country_name="GB")
        elif currency_code == 'HKD':
            m.add_country_holidays(country_name="HK")
        else:
            m.add_country_holidays(country_name=country_name)

        m.fit(df)

        future = m.make_future_dataframe(periods)

        # Eliminate weekend from future dataframe
        future['day'] = future['ds'].dt.weekday
        future = future[future['day'] <= 4]

        future['cap'] = cap
        future['floor'] = floor

        forecast = m.predict(future)

        # format graph
        fig = plot_plotly(m, forecast, trend=trend, changepoints=changepoints, **kwargs)
        fig.update_layout(title=f'{stock} {periods} days forecast')
        output = fig.show()
        return output

   def plot_future_trend_grid_search(self,
                                     stock,
                                     start_date='2021-05-01',
                                     periods=90,
                                     country_name='US',
                                     changepoints=True,
                                     trend=True,
                                     cap=1000,
                                     floor=0,
                                     growth='logistic',
                                     interval_width=0.95,
                                     **kwargs):

       """
       Function to predict the future trend of a stock using grid search.
       Note takes significantly greater compute power than plot_future_trend() function.

       Args:
           self:
           stock:
           start_date:
           periods:
           country_name:
           changepoints:
           trend:
           cap:
           floor:
           growth:
           interval_width:
           **kwargs:

       Returns: plotly figure of future trend and mean absolute error and mean absolute percentage error

       """

       post_date_df = self.stock_history.loc[~(self.stock_history['Date'] <= start_date)]
       predict_df = post_date_df.loc[post_date_df['Ticker'].isin([stock])]

       # rename columns to fit model
       df = predict_df.rename(columns={'Date': 'ds', 'Close': 'y'})
       df = df[['ds', 'y']]
       df['cap'] = cap
       df['floor'] = floor

       m = Prophet(yearly_seasonality=True, growth=growth, interval_width=interval_width)

       # get currency code for stock
       currency_code = predict_df['currency_code'].values[0]

       # HOLIDAYS - default is US
       if currency_code == 'GBP':
           m.add_country_holidays(country_name="GB")
       elif currency_code == 'HKD':
           m.add_country_holidays(country_name="HK")
       else:
           m.add_country_holidays(country_name=country_name)

       m.fit(df)

       future = m.make_future_dataframe(periods)

       # Eliminate weekend from future dataframe
       future['day'] = future['ds'].dt.weekday
       future = future[future['day'] <= 4]

       future['cap'] = cap
       future['floor'] = floor

       forecast = m.predict(future)

       # format graph
       fig = plot_plotly(m,
                         forecast,
                         trend=trend,
                         changepoints=changepoints,
                         **kwargs)

       fig.update_layout(title=f'{stock} {periods} days forecast')
       output = fig.show()

       # get Mean Absolute Error
       df_merge = pd.merge(df, forecast[['ds', 'yhat_lower', 'yhat_upper', 'yhat']], on='ds')
       df_merge = df_merge[['ds', 'yhat_lower', 'yhat_upper', 'yhat', 'y']]
       # calculate MAE between observed and predicted values
       y_true = df_merge['y'].values
       y_pred = df_merge['yhat'].values
       mae = mean_absolute_error(y_true, y_pred)
       mape = mean_absolute_percentage_error(y_true, y_pred)

       print(
           f'The Mean Absolute Error is: {"{:.2f}".format(mae)} '
           f'\nThe Mean Absolute Percentage Error is: {"{:.2f}".format(mape)} ')

       return output
