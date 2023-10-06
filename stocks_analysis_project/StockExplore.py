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
import logging
import pandas as pd
import yfinance as yf
from typing import Optional


class ExploreStocks:
    """
    ExploreStocks class for downloading and preprocessing stock data.

    Args:
        stock_list: List of stock tickers to explore.
        period: Time period to download historical data for (e.g., '1y', '2y', 'max').

    Attributes:
        stock_list: List of stock tickers to explore.
        period: Time period to download historical data for.
        currency_df: DataFrame for currency data.
        stock_history: DataFrame for stock history data.
        logger: Logger object for logging.

    Methods:
        __init__: Initializes the ExploreStocks class.
        _initialize_logging: Initializes the logger for the class.
        _download_and_preprocess_data: Downloads and preprocesses data for the specified stocks.
        _download_initial_stock_info: Downloads initial stock information for the specified stocks.
        _extract_currency_data: Extracts currency data for the specified stocks.
        _download_exchange_rates: Download exchange rates for currencies used in the stock list.
        _merge_exchange_rates_with_master: Merge historical stock data with exchange rates based on date and currency code.
        return_df: Returns the stock history DataFrame.
        plot_stock_price: Plots the stock price for each stock over time.
        plot_trade_volume: Plots the volume traded for each stock over time.
        plot_volatility: Plots the volatility of each stock (daily close % change).
        plot_cumulative_returns: Plots the cumulative return of each stock over time.
        plot_rolling_average: Plots the rolling average of each stock over time.
        plot_future_trend: Predicts and plots the future trend of a stock.
        plot_future_trend_grid_search: Predicts and plots the future trend of a stock using grid search.

    Example:
        ```
        stock_list = ['AAPL', 'MSFT']
        period = '1y'
        explorer = ExploreStocks(stock_list, period)
        explorer.plot_stock_price()
        ```
    """

    def __init__(self, stock_list: list[str], period: str):
        """
        Initializes the ExploreStocks class.

        Args:
            stock_list: List of stock tickers to explore.
            period: Time period to download historical data for (e.g., '1y', '2y', 'max').

        Attributes:
            stock_list: List of stock tickers to explore.
            period: Time period to download historical data for.
            currency_df: DataFrame for currency data.
            stock_history: DataFrame for stock history data.
            logger: Logger object for logging.

        Example:
            ```
            stock_list = ['AAPL', 'MSFT']
            period = '1y'
            explorer = ExploreStocks(stock_list, period)
            ```
        """

        self.stock_list = stock_list
        self.period = period
        self.currency_df = None
        self.stock_history = None

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.NOTSET)

        self.logger = self._initialize_logging()  # Call the logging setup method

        self._download_and_preprocess_data()

    def _initialize_logging(self):
        """
        Initializes the logging for the class.

        Returns:
            logger: Logger object for logging.

        Example:
            ```
            explorer = ExploreStocks()
            logger = explorer._initialize_logging()
            ```
        """

        # Set up logger to console
        console = logging.StreamHandler()
        console.setLevel(logging.ERROR)
        console_format = "%(asctime)s | %(levelname)s: %(message)s"
        console.setFormatter(logging.Formatter(console_format))
        self.logger.addHandler(console)

        # Set upself.logger to file
        file_handler = logging.FileHandler("ExploreStocks.log")
        file_handler.setLevel(logging.INFO)
        file_handler_format = "%(asctime)s | %(levelname)s | %(lineno)d: %(message)s"
        file_handler.setFormatter(logging.Formatter(file_handler_format))
        self.logger.addHandler(file_handler)

        return self.logger

    def _download_and_preprocess_data(self):
        """
        Downloads and preprocesses data for the specified stocks.
        """
        self._download_initial_stock_info()

        self._extract_currency_data()  # - error here

        self._download_exchange_rates()

        self._merge_exchange_rates_with_master()

        self.logger.info("Data download and preprocessing completed.")

    def _download_initial_stock_info(self):
        """
        Downloads initial stock information for the specified stocks.
        """

        try:
            # Download historical stock data for the specified stocks
            stocks_df = yf.download(self.stock_list, group_by="Ticker", period="max")
            stocks_df = (
                stocks_df.stack(level=0)
                .rename_axis(["Date", "Ticker"])
                .reset_index(level=1)
            )
            stocks_df = stocks_df.reset_index()
        except Exception as e:
            self.logger.error("Error getting stock data", e)

        self.logger.info("Initial Stock Information Downloaded")

        self.stock_history = stocks_df.copy()

    def _extract_currency_data(self):
        """
        Extracts currency data for the specified stocks.
        """
        currency_code = {}

        for ticker in self.stock_list:
            try:
                tick = yf.Ticker(ticker)
                currency_code[ticker] = tick.info["currency"]
            except Exception as e:
                self.logger.error("Error getting currency symbol", e)

        currency_code_df = pd.DataFrame(
            list(currency_code.items()), columns=["Ticker", "currency_code"]
        )
        currency_code_df["currency_code"] = currency_code_df[
            "currency_code"
        ].str.upper()

        self.currency_df = currency_code_df.copy()

        self.stock_history = pd.merge(
            self.stock_history,
            self.currency_df,
            left_on=["Ticker"],
            right_on=["Ticker"],
            how="left",
        )

        self.logger.info("Currency Extracted")

    def _download_exchange_rates(self):
        """
        Download exchange rates for currencies used in the stock list.

        This function downloads the exchange rates for the currencies used in the stock list
        for the specified period.

        Args:
            None

        Returns:
            None
        """

        # List of unique currency codes from the currency dataframe
        currencylist = [
            x.upper() for x in (list(self.currency_df.currency_code.unique()))
        ]

        interval = "1d"

        # Create a metadata dataframe containing information about currency pairs and their tickers
        meta_df = pd.DataFrame(
            {
                "FromCurrency": [x for x in currencylist],
                "ToCurrency": ["GBP" for a in currencylist],
                "YahooTickers": [f"{a}GBP=X" for a in currencylist],
            }
        )

        # Download exchange rate data for each currency pair and concatenate the data
        currency_df = pd.DataFrame(
            yf.download(
                tickers=meta_df["YahooTickers"].values[0],
                period=self.period,
                interval=interval,
            ),
            columns=["Open", "Close", "Low", "High"],
        ).assign(
            FromCurrency=meta_df["FromCurrency"].values[0],
            ToCurrency=meta_df["ToCurrency"].values[0],
        )
        for i in range(1, len(meta_df)):
            try:
                currency_help_df = pd.DataFrame(
                    yf.download(
                        tickers=meta_df["YahooTickers"].values[i],
                        period=self.period,
                        interval=interval,
                    ),
                    columns=["Open", "Close", "Low", "High"],
                ).assign(
                    FromCurrency=meta_df["FromCurrency"].values[i],
                    ToCurrency=meta_df["ToCurrency"].values[i],
                )
                currency_df = pd.concat([currency_df, currency_help_df])
            except Exception as e:
                logger.error("Error getting exchange rates", e)

        # Reset the index of the currency dataframe and update the class attribute
        currency_df = currency_df.reset_index()

        self.currency_df = currency_df.copy()

        # Log the completion of the exchange rate retrieval process
        self.logger.info("Exchange Rates Obtained")

    def _merge_exchange_rates_with_master(self):
        """
        Merge historical stock data with exchange rates based on date and currency code.

        This function performs the merging of historical stock data and exchange rates
        to calculate the calculated GBP close for each stock.

        Args:
            None

        Returns:
            None
        """

        # Merge stock history with currency data using date and currency code

        # retain original data
        self.stock_history_org = self.stock_history.copy()

        # Rename and convert currency close column to numeric type
        self.currency_df = self.currency_df.rename(
            columns={"Close": "currency_close"}
        ).reset_index()
        self.currency_df["currency_close"] = pd.to_numeric(
            self.currency_df["currency_close"], downcast="float", errors="coerce"
        )

        # assign correct data types to join
        self.stock_history["Date"] = self.stock_history["Date"].astype("datetime64[ns]")
        self.currency_df["Date"] = self.currency_df["Date"].astype("datetime64[ns]")
        self.stock_history["currency_code"] = self.stock_history[
            "currency_code"
        ].astype(str)
        self.currency_df["FromCurrency"] = self.currency_df["FromCurrency"].astype(str)

        # join stock history with currency exchange data
        master_df = pd.merge(
            self.stock_history,
            self.currency_df[["Date", "FromCurrency", "currency_close"]],
            left_on=["Date", "currency_code"],
            right_on=["Date", "FromCurrency"],
            how="left",
        )

        # # Rename and convert currency close column to numeric type
        # master_df = master_df.rename(columns={'Close': 'currency_close'})

        master_df["currency_close"] = pd.to_numeric(
            master_df["currency_close"], downcast="float", errors="coerce"
        )

        # Calculate calculated GBP close by multiplying close price with currency close
        master_df["GBP_calculated close"] = (
            master_df["Close"] * master_df["currency_close"]
        )

        # Filter out data for weekends (non-trading days)
        master_df["day"] = master_df["Date"].dt.weekday
        master_df = master_df[master_df["day"] <= 4]

        # Calculate the percentage of NaN values in the calculated GBP column
        na_count = master_df["currency_close"].isna().sum() / master_df.shape[0] * 100
        logger.info(
            f'\n% of NaN values in calculated GBP column: {"{:.2f}".format(na_count)}'
        )

        # Replace NaN values in the calculated GBP column for GBP currency with 1
        master_df.loc[master_df["currency_code"].isin(["GBP"]), "currency_close"] = 1

        # Update the stock history dataframe with the merged data
        self.stock_history = master_df.copy()

        # Log the completion of the data merging process
        self.logger.info("Data Retrieved - dataframe with exchange rates initialised")

    def return_df(self):
        return self.stock_history

    def plot_stock_price(self, log=False, **kwargs):
        """
        Args:
            log:
            **kwargs:

        Returns: plotly plot of the stock price for each stock over time
        """

        # plot stock price over time
        fig = px.line(
            self.stock_history.sort_values(by=["Date"], ascending=[True]).dropna(
                subset=["GBP_calculated close"]
            ),
            x="Date",
            y="GBP_calculated close",
            color="Ticker",
            title="Stock Price Over Time",
            **kwargs,
        )

        # Add Date slider
        fig.update_layout(
            xaxis_rangeselector_buttons=[
                dict(label="1m", count=1, step="month", stepmode="backward"),
                dict(label="6m", count=6, step="month", stepmode="backward"),
                dict(label="YTD", count=1, step="year", stepmode="todate"),
                dict(label="1y", count=1, step="year", stepmode="backward"),
                dict(step="all"),
            ]
        )

        fig.update_layout(xaxis_rangeslider_visible=True)
        fig.update_yaxes(title_text="GBP Calculated Close")

        if log:
            fig.update_yaxes(type="log", tickformat=".1e")
        return fig

    def plot_trade_volume(self, **kwargs):
        """
        Args:
            **kwargs:

        Returns: Plot of the volume traded for each stock over time"""

        return px.line(
            self.stock_history.sort_values(by=["Date"], ascending=[True]),
            x="Date",
            y="Volume",
            color="Ticker",
            facet_col="Ticker",
            title="Volume Traded Over Time",
            **kwargs,
        )

    def plot_volatility(self, **kwargs):
        """
        Args:
            **kwargs:

        Returns: plot of the volatility of each stock (daily close % change)
        """

        # compute daily percent change in closing price
        self.stock_history["returns"] = self.stock_history.groupby("Ticker")[
            "Close"
        ].pct_change()

        # get title
        title = (
            " ".join([str(item) for item in self.stock_list])
        ) + " Daily Volatility Comparison"

        # plot histogram
        fig = px.histogram(
            self.stock_history.dropna(subset=["returns"]),
            x="returns",
            title=title,
            color="Ticker",
            **kwargs,
            nbins=200,
        )
        fig.update_yaxes(title_text="Count")
        fig.update_xaxes(title_text="Return Bins")
        return fig

    def plot_cumulative_returns(self, **kwargs):
        """
        Args:
            **kwargs:

        Returns: plot of the cumulative return of each stock over time
        """

        # compute cumulative returns
        cum_returns = self.stock_history[["Date", "Close", "Ticker"]]

        # pivot table
        cum_returns = pd.pivot_table(cum_returns, columns=["Ticker"], index=["Date"])

        # compute cumulative returns pct change
        daily_pct_change = cum_returns.pct_change()
        daily_pct_change.fillna(0, inplace=True)
        cumprod_daily_pct_change = (1 + daily_pct_change).cumprod()

        cumprod_daily_pct_change.columns = [
            "_".join([str(index) for index in multi_index])
            for multi_index in cumprod_daily_pct_change.columns.ravel()
        ]
        cumprod_daily_pct_change = cumprod_daily_pct_change.reset_index()

        # get title
        title = (
            " ".join([str(item) for item in self.stock_list])
        ) + " Cumulative Returns"

        # plot
        fig = px.line(
            cumprod_daily_pct_change.sort_values(by=["Date"], ascending=[True]),
            x="Date",
            y=["Close_0293.HK", "Close_AF.PA", "Close_IAG.L"],
            title=title,
            **kwargs,
        )

        fig.update_yaxes(title_text="Cumulative Returns")
        fig.update_layout(xaxis_rangeslider_visible=True)
        fig.update_layout(legend_title_text="Ticker")

        return fig

    def plot_rolling_average(self, **kwargs):
        """
        Args:
            **kwargs:

        Returns: plot of the rolling average of each stock over time
        """

        # compute several rolling means
        gbp_df = self.stock_history.copy()

        gbp_df["MA50"] = gbp_df.groupby("Ticker")["GBP_calculated close"].transform(
            lambda x: x.rolling(50, 25).mean()
        )

        gbp_df["MA200"] = gbp_df.groupby("Ticker")["GBP_calculated close"].transform(
            lambda x: x.rolling(200, 100).mean()
        )

        gbp_df["MA365"] = gbp_df.groupby("Ticker")["GBP_calculated close"].transform(
            lambda x: x.rolling(365, 182).mean()
        )

        gbp_df["MA1000"] = gbp_df.groupby("Ticker")["GBP_calculated close"].transform(
            lambda x: x.rolling(1000, 500).mean()
        )

        # plot rolling means over time
        fig = px.line(
            gbp_df.sort_values(by=["Date"], ascending=[True]).dropna(
                subset=["MA1000", "GBP_calculated close", "MA200"]
            ),
            x="Date",
            y=["MA1000", "MA200", "GBP_calculated close"],
            facet_row="Ticker",
            title="Rolling Mean Stock Price Over Time",
            **kwargs,
        )

        # add custom y-axis for each facet
        #         for k in fig.layout:
        #             if re.search('yaxis[1-9]+', k):
        #                 fig.layout[k].update(matches=None)

        # label y-axis
        fig["layout"]["yaxis1"]["title"]["text"] = ""
        fig["layout"]["yaxis2"]["title"]["text"] = "Stock Value in GBP"
        fig["layout"]["yaxis3"]["title"]["text"] = ""
        fig.update_yaxes(matches=None)

        # add Date slider
        fig.update_layout(
            xaxis_rangeselector_buttons=[
                dict(label="1m", count=1, step="month", stepmode="backward"),
                dict(label="6m", count=6, step="month", stepmode="backward"),
                dict(label="YTD", count=1, step="year", stepmode="todate"),
                dict(label="1y", count=1, step="year", stepmode="backward"),
                dict(step="all"),
            ]
        )
        fig.update_layout(xaxis_rangeslider_visible=True)
        fig.update_layout(legend_title_text="Average")

        return fig

    def plot_future_trend(
        self,
        stock,
        start_date="2021-05-01",
        periods=90,
        country_name="US",
        changepoints=True,
        trend=True,
        cap=1000,
        floor=0,
        growth="logistic",
        interval_width=0.95,
        **kwargs,
    ):
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

        # filter dataframe to get stock data after start date
        post_date_df = self.stock_history.loc[
            ~(self.stock_history["Date"] <= start_date)
        ]
        predict_df = post_date_df.loc[post_date_df["Ticker"].isin([stock])]

        # rename columns to fit model
        df = predict_df.rename(columns={"Date": "ds", "Close": "y"})
        df = df[["ds", "y"]]
        df["cap"] = cap
        df["floor"] = floor

        # construct model
        m = Prophet(
            yearly_seasonality=True, growth=growth, interval_width=interval_width
        )

        # get currency code for stock
        currency_code = predict_df["currency_code"].values[0]

        # assign HOLIDAYS - default is US
        if currency_code == "GBP":
            m.add_country_holidays(country_name="GB")
        elif currency_code == "HKD":
            m.add_country_holidays(country_name="HK")
        else:
            m.add_country_holidays(country_name=country_name)

        # fit model
        m.fit(df)

        # construct future dataframe
        future = m.make_future_dataframe(periods)

        # Eliminate weekend from future dataframe
        future["day"] = future["ds"].dt.weekday
        future = future[future["day"] <= 4]

        future["cap"] = cap
        future["floor"] = floor

        forecast = m.predict(future)

        # format graph
        fig = plot_plotly(m, forecast, trend=trend, changepoints=changepoints, **kwargs)
        fig.update_layout(title=f"{stock} {periods} days forecast")
        return fig.show()

    def plot_future_trend_grid_search(
        self,
        stock,
        start_date="2021-05-01",
        periods=90,
        country_name="US",
        changepoints=True,
        trend=True,
        cap=1000,
        floor=0,
        growth="logistic",
        interval_width=0.95,
        **kwargs,
    ):
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

        # filter dataframe to get stock data after start date

        post_date_df = self.stock_history.loc[
            ~(self.stock_history["Date"] <= start_date)
        ]
        predict_df = post_date_df.loc[post_date_df["Ticker"].isin([stock])]

        # rename columns to fit model
        df = predict_df.rename(columns={"Date": "ds", "Close": "y"})
        df = df[["ds", "y"]]
        df["cap"] = cap
        df["floor"] = floor

        # construct model
        m = Prophet(
            yearly_seasonality=True, growth=growth, interval_width=interval_width
        )

        # get currency code for stock
        currency_code = predict_df["currency_code"].values[0]

        # HOLIDAYS - default is US
        if currency_code == "GBP":
            m.add_country_holidays(country_name="GB")
        elif currency_code == "HKD":
            m.add_country_holidays(country_name="HK")
        else:
            m.add_country_holidays(country_name=country_name)

        # fit model
        m.fit(df)

        # construct future dataframe
        future = m.make_future_dataframe(periods)

        # Eliminate weekend from future dataframe
        future["day"] = future["ds"].dt.weekday
        future = future[future["day"] <= 4]

        # add cap and floor to limit extent of trend
        future["cap"] = cap
        future["floor"] = floor

        # predict future
        forecast = m.predict(future)

        # format graph
        fig = plot_plotly(m, forecast, trend=trend, changepoints=changepoints, **kwargs)

        fig.update_layout(title=f"{stock} {periods} days forecast")
        output = fig.show()

        # get Mean Absolute Error
        df_merge = pd.merge(
            df, forecast[["ds", "yhat_lower", "yhat_upper", "yhat"]], on="ds"
        )
        df_merge = df_merge[["ds", "yhat_lower", "yhat_upper", "yhat", "y"]]

        # calculate MAE between observed and predicted values
        y_true = df_merge["y"].values
        y_pred = df_merge["yhat"].values
        mae = mean_absolute_error(y_true, y_pred)
        mape = mean_absolute_percentage_error(y_true, y_pred)

        print(
            f'The Mean Absolute Error is: {"{:.2f}".format(mae)} '
            f'\nThe Mean Absolute Percentage Error is: {"{:.2f}".format(mape)} '
        )

        return output
