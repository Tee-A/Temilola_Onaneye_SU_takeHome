## Task 1

The dimensional modeling design can be found here: ![Dimensional Model](<dimensional model (star schema).png>)

The ETL script for the dataset can be found here: [ETL Script](airflow/dags/etl.py)


## Task 2

The optimized sql query can be found here: [Optimized Query](<optimized sql query.sql>). The initial query here: [Initial Query](<initial query.sql>)


## Task 3

The pthon files for spark processing can be found below:

- Initial script: [Initial script](<pyspark/apps/pyspark analysis.py>)
- Optimized script: [Optimized script](<pyspark/apps/pyspark analysis optimized.py>)


## Task 4

The dimensional modeling design can be found here: ![Dimensional Model](<dimensional model (star schema).png>)

The ERD for operational use can be found here: ![ERD](<Assignment ERD.png>)


### Appendix

# Set up

### Prerequisites for setup: Docker is setup and up and running

## Airflow Setup Steps

The necessary files are already created within the airflow folder. To spin up the airflow services, tale the following steps:

- Step 1: Open a new terminal, and cd into the airflow directory
- Step 2: Run `docker compose up --build`

The second step might take minutes if not hours, ensure to monitor the logs on the terminal to note if the services are up. To confirm if the services are up, open a new terminal and run `docker ps`, this should show airflow services running and healthy. There should be a total of about 6 services running. Most espectially the airflow-scheduler, airflow-webserver.

Once the services are up and running, you will be able to access airflow from your browser on local at localhost:8080. user: airflow password: airflow.

## Postgres database setup

Open a new terminal and run `docker run -d --name postgres_source -p5433:5432 -e POSTGRES_PASSWORD=password1234 postgres:15.6`. This will spin up a docker container called postgres_source that will host the postgres database server.

To access the server via command line, follow the steps below:

- run `docker excec -it postgres_source bash` # This will automatically switch to a bash terminal
- to connect to the server, run `psql -U postgres` # you will automatically be connected to the postgres instance.
- To create the tables needed for the etl, we will need to create a database. Run this command `CREATE DATABASE projects;`
- We will then need to create the schemas for our tables, before you do that, we will have to connect to the database we just created, run `\c projects`. Then run these commands to create the schemas separately: `CREATE SCHEMA raw;` `CREATE SCHEMA dim;` `CREATE SCHEMA facts;`

Once the above is established, use the commands in this script: [SQL Script](<sql script.sql>) to create the tables and indexes.

## Spark set up

Open a new terminal and cd into the pyspark directory. Once that is done, run this command `docker build -t my-apache-spark:3.5.1`. Once this is finished, proceed to run `docker compose up`. This will spin up the master and worker nodes for the spark environment. 

The log file analyzed in this section was generated using `faker` python library. You will need to run the python file `log_generator.py` by running this command on the terminal. Ensure you are in the directory where the python file is stored, then run `python log_generator.py`. This will save the file called `sample_logs.csv` which contains 25 million log records within `pyspark/data` directory. Ensure that you have python installed and you have also installed `faker` and `pandas` library. You can install both libraries by running `pip install pandas faker` on terminal.

### Initial configurations (In the docker compose file in the pyspark directory)

Below is what was set for the worker nodes at first:

- `SPARK_WORKER_CORES=1`
- `SPARK_EXECUTOR_MEMORY=1G`

### For the optimized script

The comfiguration was this:

- `SPARK_WORKER_CORES=2`
- `SPARK_EXECUTOR_MEMORY=2G`

When you have these parameters changed, you have to run `docker compose down` then `docker compose up`. This has to be done inside the pyspark directory.
