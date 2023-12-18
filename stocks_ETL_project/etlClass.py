import os
import logging
import time
from datetime import datetime, date
import pandas as pd
import yfinance as yf
import sqlalchemy
from sqlalchemy import create_engine, VARCHAR, DateTime, Float, String, Time
from typing import List
import sqlite3
    
    
# util func
def log_method_call(func):
    def wrapper(self, *args, **kwargs):
        method_name = func.__name__
        self.logger.info(f"Calling {method_name} with args: {args}, kwargs: {kwargs}")
        try:
            result = func(self, *args, **kwargs)
            self.logger.info(f"{method_name} executed successfully.")
            return result
        except Exception as e:
            self.logger.exception(f"Exception in {method_name}: {e}")
            raise  # Re-raise the exception after logging
    return wrapper


class StocksETL:
    def __init__(
        self,
        stock_list: List[str],
        database_type: str = "sqlite",
        db_name: str = "stock_db",
    ):
        """
        Initializes the StocksETL class.

        Args:
            stock_list: List of stock tickers to explore.
            database_type: Type of database to use ('azure_sql' or 'sqlite').
            db_name: Name of the SQLite database.

        Attributes:
            stock_list: List of stock tickers to explore.
            currency_df: DataFrame for currency data.
            stock_history: DataFrame for stock history data.
            logger: Logger object for logging.
            engine: SQLAlchemy engine for database connection.
            conn: SQLite connection object.

        Example:
            ```
            stock_list = ['AAPL', 'MSFT']
            etl = StocksETL(stock_list)
            ```
        """
        self.stock_list = stock_list
        self.currency_df = None
        self.stock_history = None

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.NOTSET)
        self.logger = self._initialize_logging()  # Call the logger setup method

        if database_type == "azure_sql":
            self.engine = self.setup_azure_sql()
        elif database_type == "sqlite":
            self.engine, self.conn = self.setup_sqlite(db_name)

        self.tickers = {stock: yf.Ticker(stock) for stock in stock_list}

    def _initialize_logging(self, filepath="stocks_ETL_project"):
        """
        Initializesself.logger for the class.
        """
        # Set upself.logger to console
        console = logging.StreamHandler()
        console.setLevel(logging.ERROR)
        console_format = "%(asctime)s | %(levelname)s: %(message)s"
        console.setFormatter(logging.Formatter(console_format))
        self.logger.addHandler(console)

        # Set upself.logger to file
        file_handler = logging.FileHandler(f"{filepath}/dataExtract.log")
        file_handler.setLevel(logging.INFO)
        file_handler_format = "%(asctime)s | %(levelname)s | %(lineno)d: %(message)s"
        file_handler.setFormatter(logging.Formatter(file_handler_format))
        self.logger.addHandler(file_handler)

        return self.logger

    def setup_sqlite(self, db_name: str = "stock_db"):
        """
        Function to set up an SQLite connection and create the database if it doesn't exist.

        Args:
            db_name: Name of the SQLite database.

        Returns:
            engine: SQLAlchemy engine for database connection.
            conn: SQLite connection object.
        """
        conn = sqlite3.connect(f"stocks_ETL_project/{db_name}.db")

        engine = create_engine(f"sqlite:///stocks_ETL_project/{db_name}.db", echo=True)

        return engine, conn

    def setup_azure_sql(self, driver = "ODBC Driver 17 for SQL Server"):
        """
        Function to set up the SQL connection.

        Arguments:
            None

        Return:
           self.engine: SQLAlchemyself.engine for database connection
        """
        # Check if the required environment variables are set
        required_env_vars = [
            "SQL_USERNAME",
            "SQL_PASSWORD",
            "SQL_SERVER",
            "SQL_DATABASE",
        ]

        if missing_Vars := [
            env_var for env_var in required_env_vars if env_var not in os.environ
        ]:
            raise ValueError(f"Environment variables '{missing_Vars}' are not set.")

        # Set up database connection
        sql_username = os.environ["SQL_USERNAME"]
        sql_password = os.environ["SQL_PASSWORD"]
        sql_server = os.environ["SQL_SERVER"]
        sql_database = os.environ["SQL_DATABASE"]

        odbc_str = f"Driver={driver};Server={sql_server},1433;Database={sql_database};Uid={sql_username};Pwd={sql_password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        connect_str = f"mssql+pyodbc:///?odbc_connect={odbc_str}"

        return create_engine(connect_str, fast_executemany=True)

    @log_method_call
    def send_dataframe_to_sql(self, df, table_name, if_exists="append", dtype=None):
        """
        Function to send a dataframe to SQL database.
        
        Args:
            df: DataFrame to be sent to the SQL database.
            table_name: Name of the table in the SQL database.
            if_exists: Action to take if the table already exists in the SQL database.
                       Options: "fail", "replace", "append" (default: "append").
            dtype: Dictionary of column names and data types to be used when creating the table (default: None).
        
        Returns:
            None. This function logs a note in the log file to confirm that data has been sent to the SQL database.
        """
        
        # Send to SQL with SQL Alchemy
        df.to_sql(
            table_name,
            self.engine,
            if_exists=if_exists,
            index=False,
            dtype=dtype
        )
    
    @log_method_call
    def get_stock_history(self, stock):
        """
        Function to pull historical stock data for a given stock.

        Arguments:
            stock: individual stock in which user wants to return data for.

        Return:
            Pandas DataFrame with historical stock data.
        """
        stock_history = self.tickers[stock].history(period="max").reset_index()
        stock_history = stock_history.rename(str.lower, axis="columns")
        stock_history["stock"] = stock
        stock_history["date"] = pd.to_datetime(stock_history["date"])
        stock_history = stock_history[
            ["date", "open", "high", "low", "close", "volume", "dividends", "stock"]
        ]
        return stock_history

    @log_method_call
    def get_major_shareholders(self, stock):
        """
        Function to pull major shareholders data for a given stock.

        Arguments:
            stock: individual stock in which user wants to return data for.

        Return:
            Pandas DataFrame with major shareholders data.
        """
        major_share_holders = self.tickers[stock].major_holders
        major_share_holders = major_share_holders.rename(
            columns={0: "percent", 1: "detail"}
        )
        major_share_holders["stock"] = stock
        return major_share_holders

    @log_method_call
    def get_stock_financials(self, stock):
        """
        Function to pull financials data for a given stock.

        Arguments:
            stock: individual stock in which user wants to return data for.

        Return:
            Pandas DataFrame with financials data.
        """
        stock_financials = self.tickers[stock].financials.transpose().reset_index()
        stock_financials.columns.values[0] = "date"
        stock_financials["stock"] = stock
        return stock_financials

    @log_method_call
    def get_news(self, stock):
        """
        Function to pull news data for a given stock.

        Arguments:
            stock: individual stock in which user wants to return data for.

        Return:
            Pandas DataFrame with news data.
        """
        news_list = self.tickers[stock].news
        column_names = [
            "stock",
            "uuid",
            "title",
            "publisher",
            "link",
            "provider_publish_time",
            "type",
        ]
        news_df = pd.DataFrame(columns=column_names)
        news_df["uuid"] = [x["uuid"] for x in news_list]
        news_df["title"] = [x["title"] for x in news_list]
        news_df["publisher"] = [x["publisher"] for x in news_list]
        news_df["link"] = [x["link"] for x in news_list]
        news_df["type"] = [x["type"] for x in news_list]
        news_df["stock"] = stock
        return news_df

    @log_method_call
    def combined_stock_sql_send(self, stock):
        """
        Function to pull stock information. Currently pulls historical stock data, major shareholders,
        earnings, quarterly earnings and news.

        Arguments:
            stock: individual stock in which user wants to return data for.

        Return:
            None.
        """
        
        stock_history = self.get_stock_history(stock)
        self.send_dataframe_to_sql(stock_history, "stock_history")
        self.logger.info(f"historical {stock} data sent to sql")
        stock_max_date = str(stock_history.date.max())

        # Open the file using the 'with' statement
        with open("last_update.txt", "w") as f:
            f.write(f"{stock}_date_max {stock_max_date}")

        major_share_holders = self.get_major_shareholders(stock)
        self.send_dataframe_to_sql(major_share_holders, "major_share_holders")

        stock_financials = self.get_stock_financials(stock)
        self.send_dataframe_to_sql(stock_financials, "financials")

        news_df = self.get_news(stock)
        self.send_dataframe_to_sql(news_df, "news")

    def combined_tables(self):
        """
        Function to pull stock information for multiple stocks. Currently pulls historical stock data, major shareholders,
        earnings, quarterly earnings and news.

        Arguments:
            # companies: list of stocks that we want to return data for

        Return:
            # returns note in log file to confirm data has been sent to SQL database
        """

        try:
            self.blank_sql()
        except Exception as e:
            self.logger.exception("An exception occurred clearing tables: %s", e)

        # create master table
        stock_df = pd.DataFrame(columns=["stock"])
        stock_df["stock"] = list(self.stock_list)
        self.send_dataframe_to_sql(stock_df,
                                   "stocks_master", 
                                   dtype=self.sqlcol(stock_df))

        # get tables for each individual stock
        list(map(self.combined_stock_sql_send, self.stock_list))

        return self.logger.info("SQL Updated with combined tables")

    def get_last_update_date(self, stock):
        """
        Function to retrieve the last update date for a stock
        Arguments:
            stock: individual stock to retrieve the last update date for
        Returns:
            last_update_date: the last update date for the specified stock
        """
        try:
            with open("last_update.txt", "r") as f:
                lines = f.readlines()
                for line in lines:
                    if f"{stock}_date_max" in line:
                        return line.split(" ")[-1]
        except FileNotFoundError:
            return None

    def stock_history_updater(self, stock):
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
        msft = self.tickers[stock]
        
        try:
            # return historical stock data and send to sql db
            today_date = date.today()
            stock_history = msft.history(
                start=self.get_last_update_date(stock), end=today_date, interval="1d"
            ).reset_index()

            # minor cleaning
            stock_history = stock_history.rename(str.lower, axis="columns")
            stock_history["stock"] = stock
            stock_history["date"] = pd.to_datetime(stock_history["date"])

            # send to SQL with SQL Alchemy
            self.send_dataframe_to_sql(stock_history, "stock_history")
            stock_max_date = str(stock_history.date.max())

            # Set new dates to limit size of future uploads
            with open("last_update.txt", "w") as f:
                f.write(f"{stock}_date_max {stock_max_date}")

        except Exception as e:
            self.logger.error("An exception occurred: %s", e)

    def exchange_rate_table(self, period, interval="1d"):
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
        for ticker in self.stock_list:
            try:
                tick = self.tickers[ticker]

                currency_code[ticker] = tick.info["currency"]
            except Exception as e:
                self.logger.error("Error getting currency symbol", e)

        # make dataframe
        df = pd.DataFrame(
            list(currency_code.items()), columns=["Ticker", "currency_code"]
        )
        df["currency_code"] = df["currency_code"].apply(lambda x: x.upper())

        # Create a Datafame with Yahoo Finance Module
        currencylist = [
            x.upper() for x in (list(df.currency_code.unique()))
        ]  # choose currencies

        meta_df = pd.DataFrame(
            {
                "FromCurrency": list(currencylist),
                "ToCurrency": ["GBP" for a in currencylist],
                "YahooTickers": [f"{a}GBP=X" for a in currencylist],
            }
        )
        
        currency_df = pd.DataFrame(
            yf.download(
                tickers=meta_df["YahooTickers"].values[0],
                period=period,
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
                        period=period,
                        interval=interval,
                    ),
                    columns=["Open", "Close", "Low", "High"],
                ).assign(
                    FromCurrency=meta_df["FromCurrency"].values[i],
                    ToCurrency=meta_df["ToCurrency"].values[i],
                )

                currency_df = pd.concat([currency_df, currency_help_df])
                currency_df = currency_df.reset_index()
                self.logger.info("Exchange Rates Obtained")

            except Exception as e:
                self.logger.error("Error getting exchange rates", e)

        # use exchange rates table with daily increments to crate date dimension table
        date_df = pd.DataFrame()
        date_df["date_key"] = currency_df["Date"].dt.strftime("%m%d%Y")
        date_df["year"] = currency_df["Date"].dt.year
        date_df["quarter"] = currency_df["Date"].dt.quarter
        date_df["month"] = currency_df["Date"].dt.month

        self.send_dataframe_to_sql(date_df, "date_dimension")

        # create unique id
        currency_df["exchange_id"] = currency_df["Date"].dt.strftime("%m%d%Y") + (
            currency_df["FromCurrency"]
        )
        currency_df["currency_date_key"] = currency_df["Date"].dt.strftime("%m%d%Y")

        # rename columns
        currency_df.rename(columns={"Close": "currency_close"}, inplace=True)

        # merge exchange rates with master dataframe
        exchange_df = pd.merge(
            df,
            currency_df,
            how="right",
            left_on=["currency_code"],
            right_on=["FromCurrency"],
        )
        self.send_dataframe_to_sql(exchange_df, "exhange_table")

        return self.logger.info("Exchange rate table created in SQL database")

    def blank_sql(self):
        """
        Function to clear database prior to new batch import.
        To be replaced with drop() or drop_all() method.
    
        Arguments:
            # empty
    
        Return:
            # returns note in log file to confirm data has been cleared in PostgreSQL database
        """
    
        table_list = [
            "earnings",
            "financials",
            "major_share_holders",
            "news",
            "quarterly_earnings",
            "stock_history",
            "stocks_master",
        ]
    
        with self.engine.raw_connection() as connection:
            with connection.cursor() as cursor:
                for table in table_list:
                    cursor.execute(f"DROP TABLE IF EXISTS {table};")
                    connection.commit()
                    self.logger.info(f"{table} dropped from database")

    def sqlcol(self, dfparam):
        dtypedict = {}
        dtypes = [str(x) for x in dfparam.dtypes.values]
        for i, j in zip(dfparam.columns, dtypes):
            if "object" in j:
                dtypedict[i] = sqlalchemy.types.VARCHAR(
                    length=max(dfparam[i].apply(lambda x: len(str(x))))
                )

            if "datetime" in j:
                dtypedict[i] = sqlalchemy.types.DateTime()

            if "Float" in j:
                dtypedict[i] = sqlalchemy.types.Float(precision=3, asdecimal=True)

            if "int" in j:
                dtypedict[i] = sqlalchemy.types.INT()

        return dtypedict


if __name__ == "__main__":
    # extract data and send to SQL database - specify three airline stocks foe example.

    # set environment variables
    # os.environ['SQL_USERNAME'] = ""
    # os.environ["SQL_PASSWORD"] = ""
    # os.environ["SQL_SERVER"] = ""
    # os.environ["SQL_DATABASE"] = ""

    # IAG.L = International Consolidated Airlines Group, S.A.
    # 0293.HK = Cathay Pacific Airways Ltd
    # AF.PA = Air France-KLM SA

    # send individual tables to sql for each stock
    # individualTables(['IAG.L', '0293.HK', 'AF.PA'])

    # combine tables and send to sql
    stock_list = ["AAPL"]
    x = StocksETL(stock_list)

    # x.combined_tables()

    # send exchange rate table to sql
    # x.exchange_rate_table(period='1y', interval='1d')
