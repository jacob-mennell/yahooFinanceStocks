# Yahoo Finance Project 

The project contains two parts:

## Scripts for pulling data from yahoo finance module and sending to PostGreSQL Database
Data is pulled using Yahoo Finance, minor cleaning is then undertaken before sending to a SQL database 
using sqlalchemy.
Data used is from the Yahoo Finance module includes: historical stock data, major shareholders, earnings, 
quarterly earnings and news.
Extraction functions are listed in the dataExtract.py script and called in the dataExtract.py script.

The SQL database was then connected to Tableau to produce the included dashboard.

## StockExplore Module. 
This provides the following methods to analyse a list of stocks: plot_stock_price(), plot_trade_volume(), 
plot_volatility(), plot_rolling_average(), plot_cumulative_returns(), plot_future_trend(stock). These are
listed in the StockExplore.py script and called in the analysis.py and predictive_analysis.ipynb script.
Plotting of the future trend uses the Facebook Prophet Model.

Facebook Prophet Model: https://facebook.github.io/prophet/
" a modular regression model with interpretable parameters that can be intuitively adjusted 
by analysts with domain knowledge about the time series"