# airflow_pyspark
"""
### DAG DOCUMENTATION
## Scheduling PySpark Jobs
in this DAG we schedule some tasks which are : 
1] - Creating DataProc Hadoop Cluster.\n
2] - Verify the date if it is a weekday or a weekend.
3] - Due to the verification our dag gonna be oriented to branch 1 or branch 2
4] - Branche 1 and Branche 2 are PySpark jobs which are going to be executed due to the verification task.
5] - Finally the DataProc Cluster gonna be deleted. 
"""
