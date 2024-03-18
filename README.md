# 📈 Yahoo Finance Data Analysis Project

## Table of Contents
- [Introduction](#introduction)
- [Yahoo Finance ETL](#yahoo-finance-etl)
  - [Folder: stocks_ETL_project](#folder-stocks_etl_project)
  - [Dependencies](#dependencies)
  - [Airflow and Docker Integration](#airflow-and-docker-integration)
- [Stock Analysis Module](#stock-analysis-module)
  - [Dependencies](#dependencies-1)
  - [Facebook Prophet Model](#facebook-prophet-model)

## 📌 Introduction

This project is a comprehensive toolkit for extracting, transforming, and analyzing stock data from Yahoo Finance. It is divided into two main components: the Yahoo Finance ETL (Extract, Transform, Load) process and the Stock Analysis Module. The ETL process retrieves data from Yahoo Finance, cleans it, and stores it in a SQL database. The Stock Analysis Module provides tools for in-depth analysis and visualization of stock data.

## 📊 Yahoo Finance ETL

### 📁 Folder: stocks_ETL_project

Located in the `stocks_ETL_project` folder, the ETL process extracts historical stock data, major shareholders, earnings, quarterly earnings, and news for specified stocks. The data is cleaned using Python and then stored in a SQL database.

### 📚 Dependencies:

- os
- time
- datetime
- yfinance
- sqlalchemy
- pandas
- apache-airflow

### 🚀 Airflow and Docker Integration

The project utilizes Apache Airflow for workflow automation and scheduling, and Docker for containerization, ensuring consistent execution across different environments.

### Key Challenges and Solutions

##### 📂 File Sharing with Docker Volumes

**Problem:** Efficient file sharing between the host machine and Docker containers.

**Solution:**
- **Docker Volumes:** Docker Compose volumes are used to share files between the host machine and containers.
- **Automatic Path Mapping:** Docker Compose automatically maps host machine paths to corresponding container paths when defining volumes.
- **Avoiding Redundancy:** Volume paths are not included in both the Docker Compose file and the Dockerfile to prevent confusion.

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

#### 🗄️ Database Initialization

The Airflow database is initialized using the `airflow db init` command during the image build process, preparing the database schema for use.

#### 📅 Airflow Scheduler

The Airflow scheduler, started with the `airflow scheduler` command, manages task execution and scheduling.

#### 🛠️ Database Configuration

By default, Airflow uses an SQLite database for metadata storage. For production environments, a more robust database backend like PostgreSQL or MySQL is recommended. When using a non-SQLite database, the Airflow configuration (`airflow.cfg` file) should be updated with the connection details of the chosen database.

## 📈 Stock Analysis Module

Located in the `stocks_analysis_project` folder, the `StockExplore.py` file contains the `ExploreStocks` module. This module offers various methods for detailed analysis of a list of stocks:

- `plot_stock_price()`: Visualizes the stock price over time.
- `plot_trade_volume()`: Plots the trade volume over time.
- `plot_volatility()`: Visualizes the stock volatility over time.
- `plot_rolling_average()`: Plots the rolling average of the stock price.
- `plot_cumulative_returns()`: Visualizes the cumulative returns.
- `plot_future_trend(stock)`: Uses the Facebook Prophet model to forecast the future trend of a specified stock.

### 📚 Dependencies:

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

### 📉 Facebook Prophet Model

The Facebook Prophet model, a modular regression model with interpretable parameters, is used for forecasting stock trends. For more information, refer to the [Prophet Documentation](https://facebook.github.io/prophet/).

## Summary

This project offers a robust toolkit for extracting data from Yahoo Finance, performing detailed stock analyses, and visualizing trends over time. Please ensure all required modules are installed before executing the scripts.
