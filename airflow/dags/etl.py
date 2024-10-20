from datetime import datetime
import pandas as pd
import numpy as np
import os
import re
import hashlib
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Function to extract data from CSV
def extract_data(**kwargs):
    os.chdir("/opt/airflow/data")
    list_dir = os.listdir()
    pattern = 'ClickUp|Float'
    csv_pattern = '.csv'
    my_data = []
    for data in list_dir:
        if re.match(f'{pattern}', f'{data}') and re.search(f'{csv_pattern}', f'{data}'):
            my_data.append(data)
    
    df_dict = {data[:-4]: pd.read_csv(data) for data in my_data}
    # Push extracted data to XComs
    kwargs['ti'].xcom_push(key='all_df', value=df_dict)

# Fuction to load data
def transform_data(**kwargs):
    all_df_dict = kwargs['ti'].xcom_pull(task_ids='extract_data', key='all_df')
    for key in all_df_dict:
        if re.search('ClickUp', key):
            clickup_df = all_df_dict[key]
        elif re.search('Float', key):
            float_df = all_df_dict[key]

    # Transform data types
    clickup_df['Date'] = pd.to_datetime(clickup_df['Date'])
    
    float_df['Start Date'] = pd.to_datetime(float_df['Start Date'])
    float_df['End Date'] = pd.to_datetime(float_df['End Date'])

    clickup_df.columns = ['client', 'project', 'name', 'task', 'date', 'hours', 'note', 'billable']
    float_df.columns = ['client', 'project', 'role', 'name', 'task', 'start_date', 'end_date', 'estimated_hours']

    ## Creating dimensional models

    # dim_projects 
    dim_projects = float_df.groupby(['project', 'client']).agg(count=('project', 'count')).reset_index()
    dim_projects = dim_projects[['project', 'client']]
    dim_projects['project_id'] = dim_projects['project'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    dim_projects = dim_projects[['project_id', 'project', 'client']]

    # dim_team
    dim_team = float_df.groupby(['name', 'role']).agg(count=('name', 'count')).reset_index()
    dim_team = dim_team[['name', 'role']]
    dim_team['member_id'] = dim_team['name'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    dim_team = dim_team[['member_id', 'name', 'role']]

    # fct_tasks
    tasks_merged = pd.merge(clickup_df, float_df, on=['client', 'project', 'name', 'task'], how='left')
    tasks_merged['task_id'] = tasks_merged['task'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    tasks_merged['project_id'] = tasks_merged['project'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    tasks_merged['member_id'] = tasks_merged['name'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    tasks_merged = tasks_merged[['task_id', 'task', 'project_id', 'member_id', 'start_date', 'end_date', 'estimated_hours', 'date', 'hours', 'note', 'billable']]
    tasks_merged.columns = ['task_id', 'task', 'project_id', 'member_id', 'start_date', 'end_date', 'estimated_hours', 'task_date', 'task_hours', 'task_note', 'billable']
    fct_tasks = tasks_merged

    tf_df_dict = {'dim.dim_projects': dim_projects, 'dim.dim_team': dim_team, 'fact.fct_tasks': fct_tasks}
    raw_df_dict = {'raw.clickup': clickup_df, 'raw.float': float_df}

    # Push raw and transformed data to XComs
    kwargs['ti'].xcom_push(key='transformed_df', value=tf_df_dict)
    kwargs['ti'].xcom_push(key='raw_df', value=raw_df_dict)

# Generate SQL insert commands from data
def generate_insert_sql(table_name, data, columns):
    # Construct the column and placeholder strings
    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns)) # (%s ,%s)
    
    # Create the SQL INSERT statement
    sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    # Extract values from data
    values = [tuple(row[col] for col in columns) for row in data]
    
    return sql, values

def load_data(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='raw_df')
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_df')

    hook = PostgresHook(postgres_conn_id='project_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Truncate
    cursor.execute('TRUNCATE TABLE raw.clickup, raw.float')
    cursor.execute('TRUNCATE TABLE dim.dim_projects, dim.dim_team, fact.fct_tasks CASCADE')

    for key in raw_data:

        raw_data[key] = raw_data[key].replace({np.nan: None})
        df_insert_sql, df_values = generate_insert_sql(key, raw_data[key].to_dict(orient='records'), raw_data[key].columns)

        # Establish a new connection for this task
        cursor = conn.cursor()

        # Insert
        cursor.executemany(df_insert_sql, df_values)
        conn.commit()

        # For the optimized tables
        df_insert_sql, df_values = generate_insert_sql(f'{key}_opt', raw_data[key].to_dict(orient='records'), raw_data[key].columns)

        # Insert
        cursor.executemany(df_insert_sql, df_values)
        conn.commit()

        cursor.close()

    for key in transformed_data:

        transformed_data[key] = transformed_data[key].replace({np.nan: None})
        df_insert_sql, df_values = generate_insert_sql(key, transformed_data[key].to_dict(orient='records'), transformed_data[key].columns)
        # Establish a new connection for this task
        cursor = conn.cursor()

        # Insert
        cursor.executemany(df_insert_sql, df_values)
        conn.commit()
        cursor.close()
    conn.close()
    return "data inserted successfully"
    
with DAG("etl_from_local_to_postgres",
    start_date=datetime(2024, 10 ,12), 
    schedule_interval='@daily', 
    catchup=False) as dag:
    
    extract = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data
    )

    transform = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data
    )

    load = PythonOperator(
    task_id="load_data",
    python_callable=load_data
    )
    
    
    extract >> transform >> load