# Yahoo Finance Project

## Introduction

This project consists of two main parts: Yahoo Finance ETL (Extract, Transform, Load) and Stock Analysis Module. The goal is to pull data from Yahoo Finance, perform data cleaning, and store it in a SQL database. Additionally, it includes tools for analyzing and visualizing stock data.

### Yahoo Finance ETL

#### Folder: stocks_ETL_project

The Yahoo Finance ETL process involves extracting historical stock data, major shareholders, earnings, quarterly earnings, and news related to specified stocks. The data is then cleaned in Python before being sent to a SQL Database.

##### Required Modules:

- os
- time
- datetime
- yfinance
- sqlalchemy
- pandas
- apache-airflow

### Airflow Integration and Docker

In addition to the ETL process, the project integrates Apache Airflow for workflow automation and scheduling. Docker is used for containerization, providing consistency across various environments.

#### Challenges and Solutions

##### Sharing Files and Using Volumes

**Issue:** Effective file sharing between the host machine and containers using Docker Compose.

**Solution:**
- **Volumes for Sharing:** Utilize Docker Compose volumes to share files between the host machine and containers.
- **Automatic Mapping:** Docker Compose automatically maps host machine paths to corresponding container paths when defining volumes.
- **Avoid Redundancy:** Do not include volume paths in both the Docker Compose file and the Dockerfile. It's unnecessary and can lead to confusion.

**Example with Volumes:**
```yaml
version: '3'
services:
  airflow_webserver:
    build:
      context: C:/path/to/Dockerfile/
      dockerfile: Dockerfile
    ports:
      - "8080:8080"
    volumes:
      - ./airflow/dags:/usr/local/airflow/dags
    command: ["airflow", "webserver", "--port", "8080"]
```

**Example with Dockerfile Copy:**
```Dockerfile
# Copy your DAG file to the DAGs directory
COPY ./airflow/dags/hello_world_dag.py ./dags
```

Above allows for efficient file  sharing between the host machine and your containers using Docker Compose.

##### Initializing Correct Database

Ensure that the Airflow database is initialized correctly using the `airflow db init` command during image build. This prepares the database schema for use.

##### Running Airflow Scheduler

After launching the Airflow webserver, you also need to start the Airflow scheduler using the `airflow scheduler` command. The scheduler manages task execution and scheduling.

##### Default SQLite Database

By default, Airflow uses an SQLite database for its metadata storage. While SQLite is convenient for development, it may not be suitable for production setups. Consider configuring a more robust database backend, such as PostgreSQL or MySQL, for better performance and reliability in production environments.

##### Additional Configuration

When using a non-SQLite database backend, make sure to update your Airflow configuration to reflect the connection details of the chosen database. This configuration is typically done in the `airflow.cfg` file. Adjusting the database connection settings ensures that Airflow interacts with the correct database.

## Stock Analysis Module

### Folder: stocks_analysis_project

The `StockExplore.py` file contains the ExploreStocks module, offering various methods for in-depth analysis of a list of stocks:

- **`plot_stock_price()`:** Visualizes the stock price for each stock over time.
- **`plot_trade_volume()`:** Plots the trade volume of each stock over time.
- **`plot_volatility()`:** Visualizes the volatility of each stock over time.
- **`plot_rolling_average()`:** Plots the rolling average of each stock's price.
- **`plot_cumulative_returns()`:** Visualizes the cumulative returns of each stock.
- **`plot_future_trend(stock)`:** Utilizes the Facebook Prophet model to plot the future trend of a specified stock.

#### Facebook Prophet Model

The project leverages the Facebook Prophet model for forecasting the future trend of a stock. Prophet is a modular regression model with interpretable parameters that can be adjusted intuitively by analysts with domain knowledge about the time series.

For more information about the Facebook Prophet model, visit the [Prophet Documentation](https://facebook.github.io/prophet/).

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

The project provides valuable tools for extracting data from Yahoo Finance, performing comprehensive analyses on a list of stocks, and visualizing their trends over time. Ensure the required modules are installed before executing the scripts.
