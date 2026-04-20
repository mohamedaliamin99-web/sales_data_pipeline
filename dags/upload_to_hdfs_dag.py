from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='retail_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description="From local to Snowflake gold pipeline"
) as dag:

    upload_to_hdfs = BashOperator(
    task_id="upload_file",
    bash_command='docker exec retail_namenode \
                  hdfs dfs -mkdir -p /data/raw \
                  -put -f /data/Online_Retail.csv /data/raw',
   )

    silver_layer_hdfs = BashOperator(
       task_id = "silver_layer_hdfs",
       bash_command = 'docker exec retail-spark-master \
                       /opt/bitnami/spark/bin/spark-submit \
                       --master spark://spark-master:7077 /opt/spark-apps/scripts/data_preparation.py'
   )

    silver_layer_snowflake = BashOperator(
       task_id = "silver_layer_snowflake",
       bash_command = 'docker exec retail-spark-master \
                       /opt/bitnami/spark/bin/spark-submit \
                       --master spark://spark-master:7077 /opt/spark-apps/scripts/snowflake_staging.py'
   )

    dbt_run = BashOperator(
        task_id = 'dbt_run',
        bash_command= 'cd /opt/airflow/dbt/retail_sales_project && dbt run',
    )

    upload_to_hdfs >> silver_layer_hdfs >> silver_layer_snowflake >> dbt_run

