# Use the official Python image as the base image
FROM python:3.8-slim-buster

# Set environment variables for Airflow configuration
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Set the working directory
WORKDIR /usr/local/airflow

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Airflow with SQLite backend
RUN pip install apache-airflow

# Initialize the Airflow database and create admin user during image build
RUN airflow db init

RUN airflow users create --username admin --password password123 --firstname john --lastname smith --role Admin --email johnsmith@example.com

# Copy your requirements.txt file to the container
COPY requirements.txt ./

# Install the required Python packages
RUN pip install -r requirements.txt

# Copy your DAG file to the DAGs directory
# handled by the docker-compose.yml
COPY ./airflow/dags/. ./dags 

# Expose the Airflow webserver port
EXPOSE 8080

# Start the Airflow webserver
CMD ["airflow", "webserver", "--port", "8080"]