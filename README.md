# Yahoo Finance Project

The project consists of two parts:

## Data Extraction and SQL Database Integration
### Folder: python-sql-tableau_project
This part of the project involves pulling data from the Yahoo Finance module, performing minor data cleaning, and then sending it to an Azure SQL Database using the SQLAlchemy library. The data extraction functions are listed in the extractFunctions.py script, and they are called in the dataExtract.py script for execution.

Data extracted from the Yahoo Finance module includes historical stock data, major shareholders, earnings, quarterly earnings, and news related to the specified stocks. The project utilizes environment variables to securely store the credentials for accessing the Azure SQL database, which are accessed through the os module.

#### Required Modules:
- pandas
- datetime
- time
- yfinance
- sqlalchemy
- os

## StockExplore Module
### Folder: python-class-analysis_project
The StockExplore module provides the StockExplore class with several methods for analyzing a list of stocks. The methods available in the StockExplore class include:
- plot_stock_price(): Visualizes the stock price for each stock over time.
- plot_trade_volume(): Plots the trade volume of each stock over time.
- plot_volatility(): Visualizes the volatility of each stock over time.
- plot_rolling_average(): Plots the rolling average of each stock's price.
- plot_cumulative_returns(): Visualizes the cumulative returns of each stock.
- plot_future_trend(stock): Uses the Facebook Prophet model to plot the future trend of a specified stock.

#### Facebook Prophet Model
The Facebook Prophet model is utilized for plotting the future trend of a stock. Prophet is a modular regression model with interpretable parameters that can be intuitively adjusted by analysts with domain knowledge about the time series.

For more information about the Facebook Prophet model, visit: https://facebook.github.io/prophet/

#### Required Modules:
- pandas
- datetime
- time
- yfinance
- sqlalchemy
- os
- plotly
- logging
- prophet
- sklearn.metrics
- dask.distributed
- itertools

The project provides valuable tools for extracting data from Yahoo Finance, analyzing a list of stocks, and visualizing their trends over time. Please ensure the required modules are installed before executing the scripts.