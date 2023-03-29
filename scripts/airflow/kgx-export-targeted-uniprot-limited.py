import os
from datetime import datetime, timedelta
from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator

MYSQL_DATABASE_PASSWORD=os.environ.get('MYSQL_DATABASE_PASSWORD')
MYSQL_DATABASE_USER=os.environ.get('MYSQL_DATABASE_USER')
MYSQL_DATABASE_INSTANCE=os.environ.get('MYSQL_DATABASE_INSTANCE')
PR_BUCKET = os.environ.get('PR_BUCKET')
UNI_BUCKET = os.environ.get('UNI_BUCKET')
START_DATE=datetime(2022, 5, 12, 0, 0)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email': ['edgargaticaCU@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}


with models.DAG(dag_id='targeted-uniprot-limited', default_args=default_args, schedule_interval=timedelta(days=1), start_date=START_DATE, catchup=False) as dag:
    kubernetes_min_pod = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='targeted-uniprot-limited',
        name='targeted-uniprot-limited',
        namespace='default',
        image_pull_policy='Always',
        resources={'request_memory': '5000M', 'request_cpu': '1500m', 'limit_memory': '7500M', 'limit_cpu': '1900m'},
        arguments=['-k', 'targeted', '-o', 'uniprot', '-uni', UNI_BUCKET, '--limit', '5'],
        env_vars={
            'MYSQL_DATABASE_PASSWORD': MYSQL_DATABASE_PASSWORD, 
            'MYSQL_DATABASE_USER': MYSQL_DATABASE_USER, 
            'MYSQL_DATABASE_INSTANCE': MYSQL_DATABASE_INSTANCE,
        },
        affinity={
            'nodeAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            'key': 'cloud.google.com/gke-nodepool',
                            'operator': 'In',
                            'values': ['pool-1']
                        }]
                    }]
                }
            }
        },
        image='gcr.io/translator-text-workflow-dev/kgx-export:latest')
