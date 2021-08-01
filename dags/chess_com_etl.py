from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

now = datetime.now()

spark_master = "spark://spark:7077"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(now.year, now.month, now.day),
    'email': ['michel.vorwieger@circuonimcs.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='chess_com_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")

extract = SparkSubmitOperator(task_id='extract',
                              conn_id='spark_default',
                              application='/usr/local/spark/app/extract_chess_data.py',
                              name='extract',
                              conf={"spark.master": spark_master},
                              execution_timeout=timedelta(minutes=10),
                              packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
                              application_args=[
                                  "mongodb://admin:password@mongodb:27017/products_database?authSource=admin", "mivo09",
                                  "01", "2021"],
                              dag=dag
                              )

transform = SparkSubmitOperator(task_id='transform',
                                conn_id='spark_default',
                                application='/usr/local/spark/app/transform_chess_data.py',
                                name='extract',
                                conf={"spark.master": spark_master},
                                execution_timeout=timedelta(minutes=10),
                                packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
                                application_args=[
                                    "mongodb://admin:password@mongodb:27017/products_database?authSource=admin", "01",
                                    "2021"],
                                dag=dag
                                )

extract >> transform
