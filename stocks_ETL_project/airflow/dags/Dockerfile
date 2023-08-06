
# Use the official Python image as the base image
FROM python:3.8-slim-buster

# Set environment variables for Airflow configuration
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=mysql+mysqlconnector://<username>:<password>@<host>:<port>/<database_name>
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /usr/local/airflow

# Install conda
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh

# Make conda available in the PATH
ENV PATH /opt/conda/bin:$PATH

# Copy the stock_env.yaml to the container
COPY stock_env.yaml ./

# Create the conda environment from stock_env.yaml
RUN conda env create -f stock_env.yaml

# Activate the conda environment
SHELL ["/bin/bash", "-c", "source activate stock_env"]

# Install Airflow with extras
RUN pip install apache-airflow[mysql]

# Copy your DAG file to the DAGs directory
COPY python_sql_tableau_project\airflow\dags\stock_etl_dag.py ./dags/

# Expose the Airflow webserver port
EXPOSE 8080

# Start the Airflow webserver
CMD ["airflow", "webserver", "--port", "8080"]