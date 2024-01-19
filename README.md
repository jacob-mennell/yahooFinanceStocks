# Yahoo Finance Project

The project consists of two parts:

## Yahoo Finance ETL

### Folder: stocks_ETL_project
This part of the project involves pulling data from the Yahoo Finance module, performing minor data cleaning in python, before sending data to a SQL Database using the SQLAlchemy library. The data extraction functions are listed in the `etlClass.py` script, and they are executed as part of the Stocks ETL module.

Data extracted from the Yahoo Finance module includes historical stock data, major shareholders, earnings, quarterly earnings, and news related to the specified stocks. The project utilizes environment variables to securely store the credentials if the user decides to write to an Azure SQL database, which are accessed through the `os` module. Alternatively, the module writes to a `sqlite` db by default.

#### Required Modules:
- os
- time
- datetime
- yfinance
- sqlalchemy
- pandas
- apache-airflow

### Airflow Integration and Docker

In addition to the Yahoo Finance ETL process, the project utilizes Apache Airflow for workflow automation and scheduling. By converting the data extraction tasks into an Airflow DAG (Directed Acyclic Graph), you have the ability to manage and monitor data pipelines easily.

Docker provides the ability to deploy and manage the entire project. Docker allows you to containerize the project, ensuring consistent execution across various environments. Docker Compose simplifies the deployment by defining services, networks, and volumes required for the application in a single `docker-compose.yml` file.

The combination of Airflow and Docker provides a scalable and reliable solution for automating data extraction, processing, and storage. The project can be deployed to different environments with ease, allowing for efficient management and scaling of containers.

#### Challenges and Solutions

##### Sharing Files and Using Volumes

**Issue:** Understanding how to effectively share files between the host machine and containers using Docker Compose.

**Solution:**
- **Volumes for Sharing:** Use Docker Compose volumes to share files between the host machine and containers.
- **Automatic Mapping:** Docker Compose automatically maps host machine paths to corresponding container paths when defining volumes.
- **Avoid Redundancy:** Do not include volume paths in both the Docker Compose file and the Dockerfile. It's unnecessary and can lead to confusion.
- **No Need to Copy:** When using volumes, avoid copying files from the host machine into the container using the Dockerfile. Volumes allow direct access to files on the host machine within the container.
- **Alternative Approach:** If you prefer not to use volumes, you can copy files into the container using the `COPY` command in the Dockerfile. However, this approach is less flexible for development as changes in the host machine aren't immediately reflected in the container.

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

The `StockExplore.py`provides the ExploreStocks module with several methods for analyzing a list of stocks. The methods available in the ExploreStocks module include:
- `plot_stock_price()`: Visualizes the stock price for each stock over time.
- `plot_trade_volume()`: Plots the trade volume of each stock over time.
- `plot_volatility()`: Visualizes the volatility of each stock over time.
- `plot_rolling_average()`: Plots the rolling average of each stock's price.
- `plot_cumulative_returns()`: Visualizes the cumulative returns of each stock.
- `plot_future_trend(stock)`: Uses the Facebook Prophet model to plot the future trend of a specified stock.

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

The project provides valuable tools for extracting data from Yahoo Finance, analyzing a list of stocks, and visualizing their trends over time. Ensure the required modules are installed before executing the scripts.
