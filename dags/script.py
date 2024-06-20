import json
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

json_file_path = os.path.join(os.path.dirname(__file__), 'dag_config.json')

with open(json_file_path, 'r') as f:
    dag_config = json.load(f)

dag_id = dag_config['dag_id']
default_args = dag_config['default_args']
schedule_interval = dag_config['schedule_interval']

default_args['start_date'] = days_ago(0)

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval=schedule_interval
)

tasks = {}
for task_config in dag_config['tasks']:
    task_id = task_config['task_id']
    bash_command = task_config['bash_command']
    
    tasks[task_id] = BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        dag=dag
    )

for task_config in dag_config['tasks']:
    task_id = task_config['task_id']
    upstream_task_ids = task_config.get('upstream_task_ids', [])
    
    for upstream_task_id in upstream_task_ids:
        tasks[upstream_task_id] >> tasks[task_id]

globals()[dag_id] = dag