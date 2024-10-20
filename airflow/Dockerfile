FROM apache/airflow:2.8.4-python3.10
#ENV AIRFLOW__CORE__LOAD_EXAMPLES=True
#ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=my_conn_string
#USER root
#RUN apt-get update \
##    && sudo apt update && sudo apt upgrade && sudo apt-get install python3.9 \
#   && apt-get install -y --no-install-recommends \
#           vim
USER root
RUN apt update
RUN apt install git -y
RUN apt update -y && sudo apt install -y build-essential libpq-dev
RUN apt-get update \
  #&& apt-get install -y openjdk-8-jdk \
  && apt-get install -y --no-install-recommends \
         gcc \
         heimdal-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Install Java (required for Spark)
# RUN apt-get update && apt-get install -y openjdk-11-jdk

# Set JAVA_HOME environment variable
#ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
#RUN EXPORT JAVA_HOME

#RUN apt-get install vim -y
USER airflow
ENV PYTHON_VERSION=3.10
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install "apache-airflow[async,postgres,google,cncf.kubernetes,snowflake,airbyte]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
#RUN pip install "apache-airflow[async,postgres,google,cncf.kubernetes,snowflake,airbyte]==${AIRFLOW_VERSION}" -r requirements2.txt
RUN pip uninstall -y argparse

USER root
RUN mkdir -p -m 777 /opt/airflow/logs
RUN mkdir -p -m 777 /opt/airflow/dags
RUN mkdir -p -m 777 /opt/airflow/data
RUN mkdir -p -m 777 /opt/airflow/plugins
RUN mkdir -p -m 777 /opt/airflow/logs/scheduler
#RUN mkdir -p -m 777 /opt/airflow/.dbt
RUN chmod a+x /usr/bin/git
#COPY .dbt /opt/airflow/dbt
#COPY .dbt/profiles.yml /home/airflow/.dbt/profiles.yml