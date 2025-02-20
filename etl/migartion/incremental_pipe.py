import os
from datetime import datetime,timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from pipe_logging import log


# Environment Variables
DATAPROC_SA = os.getenv('DATAPROC_SA', default=None)
DATAPROC_BUCKET = os.getenv('gcs_dataproc_bucket', default=None)
DTP_PROJECT_ID = os.getenv('dtp_project_id', default=None)
ENVIRONMENT = os.getenv('environment', default=None)
GCS_LANDING_BUCKET= os.getenv('gcs_landing_bucket', default=None)
GCS_ARCHIVAL_BUCKET = os.getenv('gcs_archival_bucket_stg', default=None)
CONFIG_FILES_PARENT_DIR = os.getenv('CONFIG_FILES_PARENT_DIR', default='/home/airflow/gcs')
HADOOP_USER_DATALAKE = os.getenv('hadoop_user_datalake',default=None)
HADOOP_USER_IMPALA = os.getenv('hadoop_user_impala',default=None)
GCLOUD_LOG_NAME = os.getenv('gcloud_log_name',default=None)
CROSS_REALM_TRUST_REALM = os.getenv('cross_realm_trust_realm',default="OFFICE.CORP.COMPANY.COM")
SSH_CONN_ID = os.getenv('ssh_conn_id',default='ssh_dataproc')

# Variables
workload = 'cvm'
owner = "IBM - TEAM"
job_parallelism_factor = 2
ssh_conn_id=SSH_CONN_ID
hadoop_user = HADOOP_USER_DATALAKE
cross_realm_trust_realm=CROSS_REALM_TRUST_REALM
gcloud_log_name = GCLOUD_LOG_NAME
dataproc_cluster_id = f"cluster-{workload}-pipe-{ENVIRONMENT}"
dataproc_creation_dag_id=f"{ENVIRONMENT}_dp_config_{workload}_pipe_dag"
config_dags_file_path=f'{CONFIG_FILES_PARENT_DIR}/data/job-configs/{workload}/schema/tables/pipe_01'


REGION = "asia-southeast2"
ZONE = "asia-southeast2-c"
LOG_ERROR_STATUS = 'FAILED'
LOG_SUCCESS_STATUS = 'SUCCESS'

# DAG
dag_name=f"{ENVIRONMENT}_{workload}_incremental_pipe"
dag_start_date = days_ago(1)

args = {
    'owner': owner,
    'start_date': dag_start_date
}

dag = DAG(
    dag_id=dag_name,
    schedule_interval= '00 00 * * *', # Daily at 07:00 WIB time
    start_date=dag_start_date,
    default_args=args,
    catchup=False,
    render_template_as_native_obj=True
)


#vars datetimes 
current_time = "{{ macros.datetime.now() }}"

#bash logging
bash_log_template=f"""

	{{
		"dag_id":"{dag_name}",
        "workload":"{workload}",
		"action":"$action",
		"time":"{current_time}",
		"message":"$message",
		"status":"$status"
    }}"""


def data_load(yaml):
    
    #yaml config
    wildcard_filename_pattern = yaml['wildcard_filename_pattern']
    on_prem_database_name = yaml['on_prem_database_name']
    on_prem_table_name = yaml['on_prem_table_name']
    on_prem_main_hdfs_path = yaml['on_prem_main_hdfs_path']
    on_prem_file_format = yaml['on_prem_file_format']
    partition_schema = yaml['partition_schema']
    on_prem_date_column_partition_name = yaml['on_prem_date_column_partition_name']
    on_prem_partition_format = yaml['on_prem_partition_format']
    # on_prem_table_has_partition = yaml['on_prem_table_has_partition']
    day_partition_delay_factor = yaml['day_partition_delay_factor']
    gcp_target_bq_project_id = yaml['gcp_target_bq_project_id']
    gcp_bq_table_name = yaml['gcp_bq_table_name']
    gcp_bq_dataset_name = yaml['gcp_bq_dataset_name']
    gcp_bq_schema_fields = yaml['gcp_bq_schema_fields']

    #macros
    exec_date = "{{ macros.datetime.strptime(ds,'%Y-%m-%d') - macros.timedelta(days=0)}}"
    hive_partition_value = f"{{{{ (macros.datetime.strptime(ds,'%Y-%m-%d') - macros.timedelta(days={day_partition_delay_factor})).strftime('{on_prem_partition_format}') }}}}"
    bq_partition_value = f"{{{{ (macros.datetime.strptime(ds,'%Y-%m-%d') - macros.timedelta(days={day_partition_delay_factor})).strftime('%Y-%m-%d %H:%M:%S') }}}}"
    exec_date_nodash = '{{ ds_nodash }}'

    #file path construction for hdfs and gcs
    bq_load_id = f"bq_load_id={dag_name}-{exec_date_nodash}"
    bq_load_time = f"bq_load_time='{exec_date}'"
    curr_timestamp = "{{ macros.time.mktime(macros.datetime.strptime(ds,'%Y-%m-%d').timetuple()) | int }}"
    hdfs_file_path_reformatted = f'{on_prem_main_hdfs_path}={hive_partition_value}'
    
    gcs_file_path_prefix = f"{workload}/{hive_partition_value}/bq_pipe_{curr_timestamp}/{on_prem_database_name}/{on_prem_table_name}"
    gcs_file_path_suffix = f"{bq_load_time}/{bq_load_id}/{on_prem_date_column_partition_name}='{bq_partition_value}'"
    gcs_landing_bucket_file_path = f"gs://{GCS_LANDING_BUCKET}/{gcs_file_path_prefix}/{gcs_file_path_suffix}"
    
    source_uri = f"""{{{{ti.xcom_pull(task_ids='{on_prem_table_name}.task_reformat_source_uris_{on_prem_table_name}')}}}}"""
    bigquery_insert_job_id = f"""{{{{ti.xcom_pull(task_ids='{on_prem_table_name}.task_gcs_to_bq_load_{on_prem_table_name}')}}}}"""
    sourceUriPrefix = f"gs://{GCS_LANDING_BUCKET}/{gcs_file_path_prefix}/{partition_schema}"

    #BQL
    bql_check_count_partition = \
        f"""SELECT COUNT(*) = 0
            FROM `{gcp_target_bq_project_id}.{gcp_bq_dataset_name}.{gcp_bq_table_name}`
            WHERE {on_prem_date_column_partition_name} = TIMESTAMP('{bq_partition_value}')
        """
    
    bql_count_rows_partition = \
    f"""SELECT COUNT(*) as row_count
        FROM `{gcp_target_bq_project_id}.{gcp_bq_dataset_name}.{gcp_bq_table_name}`
        WHERE {on_prem_date_column_partition_name} = TIMESTAMP('{bq_partition_value}')
    """



    run_hdfs_distcp_command = f"""
    gsutil cp gs://{DATAPROC_BUCKET}/scripts/kinit_check.sh kinit_check.sh
    bash kinit_check.sh {hadoop_user} {cross_realm_trust_realm}
    hadoop fs -test -d {hdfs_file_path_reformatted}
    status=$?
    if [ $status == 0 ]
    then
        gcloud logging write {gcloud_log_name} '{
                bash_log_template.replace('$message',f'Starting HDFS to GCS Load for Partition {hdfs_file_path_reformatted}').
                                    replace('$status',f'{LOG_SUCCESS_STATUS}').
                                    replace('$action',f'START_COPY_FROM_HDFS_TO_GCS')
              }' --payload-type=json --severity=INFO
        echo On-prem file {hdfs_file_path_reformatted} present starting distcp load
        hadoop distcp -Dmapreduce.job.hdfs-servers.token-renewal.exclude=nameservice1-impala -overwrite { hdfs_file_path_reformatted } { gcs_landing_bucket_file_path }
        status=$?
        if [ $status == 0 ]
        then
            gcloud logging write {gcloud_log_name} '{
                bash_log_template.replace('$message',f'Completed HDFS to GCS Load for Partition {hdfs_file_path_reformatted}').
                                    replace('$status',f'{LOG_SUCCESS_STATUS}').
                                    replace('$action',f'END_COPY_FROM_HDFS_TO_GCS')
              }' --payload-type=json --severity=INFO
        else
            gcloud logging write {gcloud_log_name} '{
                bash_log_template.replace('$message',f'Error - Failed! HDFS to GCS Load for Partition {hdfs_file_path_reformatted}').
                                    replace('$status',f'{LOG_ERROR_STATUS}').
                                    replace('$action',f'END_COPY_FROM_HDFS_TO_GCS')
              }' --payload-type=json --severity=ERROR
            exit 1
        fi
    else
        gcloud logging write {gcloud_log_name} '{
                bash_log_template.replace('$message',f'Error-{hdfs_file_path_reformatted} missing').
                                    replace('$status',f'{LOG_ERROR_STATUS}').
                                    replace('$action',f'END_COPY_FROM_HDFS_TO_GCS')
              }' --payload-type=json --severity=ERROR
        echo On-prem file {hdfs_file_path_reformatted} missing
        exit 1
    fi
    """
    #python callables

    def update_job_metadata(**kwargs):

        from google.cloud import bigquery

        on_prem_database_name = kwargs['on_prem_database_name']
        on_prem_table_name = kwargs['on_prem_table_name']
        bql_count_rows_partition = kwargs['bql_count_rows_partition']
        hive_partition_value = kwargs['hive_partition_value']
        bq_load_id = kwargs['bq_load_id']
        bq_partition_value = kwargs['bq_partition_value']
        bigquery_insert_job_id = kwargs['bigquery_insert_job_id']

        bq_client = bigquery.Client(project=DTP_PROJECT_ID,location=REGION)
        query_job = bq_client.query(bql_count_rows_partition)
        bq_row_count = [row for row in query_job][0]['row_count']
        print(f'bq_row_count = {bq_row_count}')


        job_status = 'SUCCESS'
        if bigquery_insert_job_id is None:
            job_status = 'FAILED'

        bql_insert_rows_partition = \
        f"""INSERT job_metadata.job_log
            (
                hive_table,
                hive_partition,
                hive_row_count,
                bq_table,
                bq_partition,
                bq_row_count,
                load_id,
                job_status
            )
            
            VALUES (
                    '{on_prem_database_name}.{on_prem_table_name}',
                    '{hive_partition_value}',
                    -1,
                    '{on_prem_database_name}.{on_prem_table_name}',
                    '{bq_partition_value}',
                    {bq_row_count},
                    '{bq_load_id}',
                    '{job_status}'
                )
        """
        print(bql_insert_rows_partition)
        insert_job = bq_client.query(bql_insert_rows_partition)

        return bq_row_count

    def reformat_source_uris(**kwargs):

        import re
        from google.cloud import storage

        gcs_landing_bucket = kwargs["GCS_LANDING_BUCKET"]
        gcs_file_path_prefix = kwargs["gcs_file_path_prefix"]
        exec_date = kwargs["exec_date"]
        dag_name = kwargs["dag_name"]
        exec_date_nodash = kwargs["exec_date_nodash"]
        on_prem_date_column_partition_name = kwargs["on_prem_date_column_partition_name"]
        hive_partition_date_time = kwargs["hive_partition_date_time"]
        partition_schema = kwargs["partition_schema"]
        wildcard_pattern = kwargs["wildcard_pattern"]
        
        gcs_path = f"""{gcs_file_path_prefix}/bq_load_time={exec_date}/bq_load_id={dag_name}-{exec_date_nodash}/{on_prem_date_column_partition_name}={hive_partition_date_time}/"""
        storage_client = storage.Client(project=DTP_PROJECT_ID)
        blobs_iterator = storage_client.list_blobs(bucket_or_name=gcs_landing_bucket,prefix=gcs_path,delimiter=None)

        print('***************Blobs in folder***************')
        for blob in blobs_iterator:
            print(f'{blob.name}\n')

        blobs_iterator = storage_client.list_blobs(bucket_or_name=gcs_landing_bucket,prefix=gcs_path,delimiter=None)

        if 'job_id' in partition_schema:
            regx = re.compile(f'{gcs_path}job_id=[0-9]+[0-9 A-Z]*/')
            list_blobs = [uri for uri in {f'gs://{gcs_landing_bucket}/{regx.search(blob.name).group()}{wildcard_pattern}' for blob in blobs_iterator if regx.search(blob.name)}]
        else:
            regx = re.compile(gcs_path)
            list_blobs = [uri for uri in {f'gs://{gcs_landing_bucket}/{regx.search(blob.name).group()}{wildcard_pattern}' for blob in blobs_iterator if regx.search(blob.name)}]

        return list_blobs
    
    task_start_load = BashOperator(
        task_id=f'task_start_load_{on_prem_table_name}',
        bash_command=f'echo {hive_partition_value}',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    task_end_load = BashOperator(
        task_id=f'task_end_load_{on_prem_table_name}',
        bash_command=f'echo Job Ended',
        trigger_rule=TriggerRule.ALL_DONE
    )
        
    task_load_hdfs_to_gcs = SSHOperator(
        task_id=f'task_load_hdfs_to_gcs_{on_prem_table_name}',
        #ssh_conn_id=ssh_conn_id,
        ssh_hook= SSHHook(
            ssh_conn_id=ssh_conn_id,
            remote_host=f'{dataproc_cluster_id}-m.{ZONE}.c.{DTP_PROJECT_ID}.internal'
        ),
        command=run_hdfs_distcp_command,
        cmd_timeout=3600,
        do_xcom_push=True,
        trigger_rule = TriggerRule.NONE_FAILED
    )

    task_reformat_source_uris = PythonOperator(
        task_id=f'task_reformat_source_uris_{on_prem_table_name}',
        python_callable=reformat_source_uris,
        op_kwargs={
            "wildcard_pattern":wildcard_filename_pattern,
            "gcs_file_path_prefix":gcs_file_path_prefix,
            "GCS_LANDING_BUCKET":GCS_LANDING_BUCKET,
            "exec_date":exec_date,
            "dag_name":dag_name,
            "exec_date_nodash":exec_date_nodash,
            "on_prem_date_column_partition_name":on_prem_date_column_partition_name,
            "hive_partition_date_time": bq_partition_value,
            "partition_schema":partition_schema
        },
        trigger_rule = TriggerRule.NONE_FAILED
    )

    task_bq_table_partition_exists = BigQueryCheckOperator(
        task_id=f'task_bq_table_partition_exists_{on_prem_table_name}',
        sql=bql_check_count_partition,
        use_legacy_sql=False,
        location=REGION,
        on_failure_callback=log.write_fail_logs,
        on_success_callback=log.write_success_logs,
        on_execute_callback=log.write_execution_logs,
        trigger_rule = TriggerRule.NONE_FAILED
    )

    task_gcs_to_bq_load = BigQueryInsertJobOperator(
    task_id=f'task_gcs_to_bq_load_{on_prem_table_name}',
    configuration={
          "load": {
            "sourceUris": source_uri,
            "schema":{
                "fields":gcp_bq_schema_fields
            },
            "destinationTable": {
                "projectId": gcp_target_bq_project_id,
                "datasetId": gcp_bq_dataset_name,
                "tableId": gcp_bq_table_name
            },
            "createDisposition": 'CREATE_NEVER',
            "writeDisposition": 'WRITE_APPEND',
            "autodetect":False,
            "sourceFormat": on_prem_file_format,
            "hivePartitioningOptions": {
                "mode": "CUSTOM",
                "sourceUriPrefix": sourceUriPrefix       
            }
        }
    },
    location=REGION,
    project_id=DTP_PROJECT_ID,
    on_failure_callback=log.write_fail_logs,
    on_success_callback=log.write_success_logs,
    on_execute_callback=log.write_execution_logs,
    trigger_rule = TriggerRule.NONE_FAILED
    )

    task_update_job_metadata = PythonOperator(
        task_id = f'task_update_job_metadata_{on_prem_table_name}',
        python_callable = update_job_metadata,
        op_kwargs={
            'on_prem_database_name':on_prem_database_name,
            'on_prem_table_name':on_prem_table_name,
            'hive_partition_value':hive_partition_value,
            'bq_load_id':bq_load_id.split('=')[1],
            'bq_partition_value':bq_partition_value,
            'bql_count_rows_partition':bql_count_rows_partition,
            'bigquery_insert_job_id':bigquery_insert_job_id
        },
        trigger_rule = TriggerRule.ALL_DONE
    )

    task_gcs_delete_datafile = GCSDeleteObjectsOperator(
      task_id=f'task_gcs_delete_datafile_{on_prem_table_name}',
      bucket_name=GCS_LANDING_BUCKET,
      prefix=gcs_file_path_prefix,
      on_failure_callback=log.write_fail_logs,
      on_success_callback=log.write_success_logs,
      on_execute_callback=log.write_execution_logs,
      trigger_rule = TriggerRule.ALL_DONE
    )

    task_start_load >> task_bq_table_partition_exists >> task_load_hdfs_to_gcs >> task_reformat_source_uris >> task_gcs_to_bq_load 
    task_gcs_to_bq_load >> task_gcs_delete_datafile >> task_update_job_metadata >> task_end_load


def main():
    from yaml import safe_load

    with dag:

        #start pipe
        task_incremental_pipe_start = BashOperator(
            task_id='task_incremental_pipe_start',
            bash_command=f"""
            gcloud logging write {gcloud_log_name} '{
                bash_log_template.replace('$message',f'Started {workload} Incremental Pipe {dag_name}').
                                    replace('$status',f'{LOG_SUCCESS_STATUS}').
                                    replace('$action',f'START_JOB')
              }' --payload-type=json --severity=INFO
            """,
            dag=dag
        )

        # Checking DataProc Cluster
        task_dataproc_check_cluster = BashOperator(
            task_id=f'dataproc_check_cluster',
            bash_command=f'gcloud dataproc clusters describe {dataproc_cluster_id} --region={REGION}',
            retries=5,
            retry_delay= timedelta(minutes=2),
            dag=dag
        )

        task_run_dataproc_creation_dag = TriggerDagRunOperator(
            task_id = f'task_run_dataproc_creation_dag',
            trigger_dag_id=dataproc_creation_dag_id,
            trigger_rule = TriggerRule.ALL_FAILED,
            wait_for_completion=True,
            dag=dag
        )

        task_incremental_pipe_end = BashOperator(
            task_id='task_incremental_pipe_end',
            bash_command=f"""
            gcloud logging write {gcloud_log_name} '{
                bash_log_template.replace('$message',f'Completed {workload} Incremental Pipe {dag_name}').
                                    replace('$status',f'{LOG_SUCCESS_STATUS}').
                                    replace('$action',f'END_JOB')
              }' --payload-type=json --severity=INFO
            """,
            trigger_rule = TriggerRule.ALL_DONE,
            dag=dag
        )

        dag_config_files = os.listdir(config_dags_file_path) 

        #lists for parallelism
        list_table_task_group = []
        list_view_table_task_group_in_parallel = []
        for i in range(job_parallelism_factor):
            list_view_table_task_group_in_parallel.append(task_incremental_pipe_start >> task_dataproc_check_cluster >> task_run_dataproc_creation_dag)

        for dag_config_file in dag_config_files:
            if dag_config_file.endswith(".yaml") :
                with open(f"{config_dags_file_path}/{dag_config_file}", 'r') as file:
                    #data = file.read()
                    yaml_data = safe_load(file)
                
                on_prem_table_name = yaml_data['on_prem_table_name']
                
                # Generating tasks from config files
                with TaskGroup(group_id=on_prem_table_name) as tg1:
                    # Generating tasks from config files
                    data_load(yaml_data)
                    list_table_task_group.append(tg1)

            else:
                continue

        
        #update view list
        for order,table_task_group in enumerate(list_table_task_group): 
            idx = order % job_parallelism_factor
            list_view_table_task_group_in_parallel[idx] = list_view_table_task_group_in_parallel[idx] >> table_task_group


        for view_table_task_group_in_parallel in list_view_table_task_group_in_parallel:
            view_table_task_group_in_parallel >> task_incremental_pipe_end
main()