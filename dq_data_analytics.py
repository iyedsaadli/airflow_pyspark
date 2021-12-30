"""
### DAG DOCUMENTATION
## Scheduling PySpark Jobs
in this DAG we schedule some tasks which are : 
1] - Creating DataProc Hadoop Cluster.
2] - Verify the date if it is a weekday or a weekend.
3] - Due to the verification our dag gonna be oriented to branch 1 or branch 2
4] - Branche 1 and Branche 2 are PySpark jobs which are going to be executed due to the verification task.
5] - Finally the DataProc Cluster gonna be deleted. 
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator)
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator

default_arguments = {
    "owner":"saadli",
    "start_date":datetime.today()
}

#===================================================================================================================================================

def today_is(execution_date=None):
    # The execution date is a string so we need to parse it
    date = datetime.strptime(execution_date, "%Y-%m-%d")

    if date.weekday() < 5:
        return 'weekday_analytics'
    
    return 'weekend_analytics'

#===================================================================================================================================================


def weekday_subdag(parent_dag_id=None, task_id=None, schedule_interval=None, default_args=None):

    my_subdag = DAG(f'{parent_dag_id}.{task_id}', schedule_interval=schedule_interval, default_args=default_args)
    py_spark_jobs = ['avg_speed', 'avg_temperature', 'avg_tire_pressure']

    for job in py_spark_jobs:
        DataProcPySparkOperator(
                        task_id = f'{job}',
                        main= f'gs://logistic-comp-spark-data-dev/pyspark/weekday/{job}.py',
                        cluster_name= "lgstc-cluster-{{ ds_nodash }}",
                        #dataproc_pyspark_jars = 'spark-lib-jar/spark-bigquery.jar',
                        dag = my_subdag
        )
    return my_subdag

#===================================================================================================================================================


with DAG(
        "bq_analytics", 
        schedule_interval="0 20 * * *", 
        catchup=False, 
        default_args=default_arguments
        ) as dag:

    dag.doc_md = __doc__
    
    ##########################################################################################################################################

    create_hadoop_cluster = DataprocClusterCreateOperator(
                                    task_id = "create_cluster", 
                                    project_id = 'iyed-2020',
                                    cluster_name = 'lgstc-cluster-{{ ds_nodash }}', 
                                    num_workers=0,
                                    storage_bucket = 'logistic-comp-spark-data-dev',
                                    region = 'europe-north1',
                                    zone = 'europe-north1-a',
                                    image_version = '2.0-debian10',
                                    master_machine_type = "n1-standard-2",
                                    gcp_conn_id = "google_cloud_default",

    )

    create_hadoop_cluster.doc_md = """## Create Hadoop Cluster"""

    ##########################################################################################################################################


    check_date =  BranchPythonOperator(
                                    task_id = 'check_date',
                                    python_callable= today_is,
                                    op_kwargs= {'execution_date' : "{{ ds }}"}
    )

    check_date.doc_md = """## Check if it's weekday or weekend."""

    ##########################################################################################################################################


    weekday_analytics = DataProcPySparkOperator(
                                    task_id = 'weekend_analytics',
                                    main = 'logistic-comp-spark-data-dev/pyspark/weekend/gas_composition_count.py',
                                    cluster_name = 'lgstc-cluster-{{ ds_nodash }}',
                                    #dataproc_pyspark_jars = 'spark-lib-jar/spark-bigquery.jar'
    )
    weekday_analytics.doc_md = """Branch 1 PySpark Job that is going to be executed on Sanday and Saturaday."""

    ##########################################################################################################################################


    weekend_analytics = SubDagOperator(
                                    task_id = 'weekday_analytics',
                                    subdag=weekday_subdag(
                                        parent_dag_id='bq_analytics', 
                                        task_id='weekday_analytics', 
                                        schedule_interval='0 20 * * *', 
                                        default_args=default_arguments),
    )
    weekend_analytics.doc_md = """Branch 2 PySpark Job that is going to be executed on weekdays. """
    
    ##########################################################################################################################################

    delete_cluster = DataprocClusterDeleteOperator(
                                    task_id = "delete_cluster",
                                    project_id = "iyed-2020",
                                    cluster_name= "lgstc-cluster-{{ ds_nodash }}",
                                    trigger_rule = "all_done",
                                    region = 'europe-north1',
    )
    delete_cluster.doc_md = """ ## Delete the cluster to save money."""

    ##########################################################################################################################################

create_hadoop_cluster >> check_date >> [weekend_analytics, weekday_analytics] >> delete_cluster

