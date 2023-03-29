from airflow.operators.bash_operator  import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import os

MYSQL_DATABASE_PASSWORD=os.environ.get('MYSQL_DATABASE_PASSWORD')
MYSQL_DATABASE_USER=os.environ.get('MYSQL_DATABASE_USER')
MYSQL_DATABASE_INSTANCE=os.environ.get('MYSQL_DATABASE_INSTANCE')
PR_BUCKET = os.environ.get('PR_BUCKET')
UNI_BUCKET = os.environ.get('UNI_BUCKET')
START_DATE=datetime(2022, 5, 12, 0, 0)
PROJECT_ID=os.environ.get('GCP_PROJECT')
REGION=os.environ.get('DATAFLOW_REGION')

#==================DAG ARGUMENTS==============================

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'schedule_interval': '0 0 * * *',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(dag_id='kgx-export-via-ai-platform-uniprot-limited', default_args=args, catchup=False, schedule_interval='0 0 * * *')


# Note: NO_ARG is a required placeholder in the gcloud ai-platform call below. If not present, 
# the first input argument ends up missing for some reason
run_kgx_export_uniprot_limited = BashOperator(
    task_id='run_kgx_export_uniprot_limited',
    bash_command="""gcloud ai-platform jobs submit training "run_kgx_export_uniprot_limited_{{ ts_nodash }}" \
       --scale-tier custom --region "$REGION" \
       --master-image-uri "gcr.io/$PROJECT_ID/kgx-export:latest" \
       --master-machine-type n2-highmem-2 \
       -- \
       NO_ARGS \
       -k \
       targeted \
       -o \
       uniprot \
       -uni \
       $UNI_BUCKET \
       --limit \
       5
""",
    env={'MYSQL_DATABASE_PASSWORD': MYSQL_DATABASE_PASSWORD, 
         'MYSQL_DATABASE_USER': MYSQL_DATABASE_USER, 
         'MYSQL_DATABASE_INSTANCE': MYSQL_DATABASE_INSTANCE},
    dag=dag)

monitor_run_kgx_export_uniprot_limited = BashOperator(
    task_id='monitor_run_kgx_export_uniprot_limited',
    bash_command="/home/airflow/gcs/data/scripts/monitor-ai-platform-job.sh run_kgx_export_uniprot_limited_{{ ts_nodash }}",
    dag=dag)


run_kgx_export_uniprot_limited >> monitor_run_kgx_export_uniprot_limited