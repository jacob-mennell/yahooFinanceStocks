# Yahoo Finance Project

The project consists of two parts:

## Yahoo Finance ETL

### Folder: stocks_ETL_project
This part of the project involves pulling data from the Yahoo Finance module, performing minor data cleaning, and then sending it to an  SQL Database using the SQLAlchemy library. The data extraction functions are listed in the `etlClass.py` script, and they are executed as part of the Stocks ETL module.

Data extracted from the Yahoo Finance module includes historical stock data, major shareholders, earnings, quarterly earnings, and news related to the specified stocks. The project utilizes environment variables to securely store the credentials for accessing the SQL database, which are accessed through the `os` module.

#### Required Modules:
- os
- time
- datetime
- yfinance
- sqlalchemy
- pandas
- airflow

### Airflow Integration and Docker

In addition to the Yahoo Finance ETL process, the project utilizes Apache Airflow for workflow automation and scheduling. By converting the data extraction tasks into an Airflow DAG (Directed Acyclic Graph), you can manage and monitor data pipelines easily.

To deploy and manage the entire project, Docker and Docker Compose are used. Docker allows you to containerize the project, ensuring consistent execution across various environments. Docker Compose simplifies the deployment by defining services, networks, and volumes required for the application in a single `docker-compose.yml` file.

The combination of Airflow and Docker provides a scalable and reliable solution for automated data extraction, processing, and storage. The project can be deployed to different environments with ease, allowing for efficient management and scaling of containers.

## Stock Analysis Module

### Folder: stocks_analysis_project

The `StockExplore.py`provides the StockExplore class with several methods for analyzing a list of stocks. The methods available in the StockExplore class include:
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
