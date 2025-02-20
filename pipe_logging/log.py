import os
from datetime import datetime


LOG_ERROR_STATUS = 'FAILED'
LOG_SUCCESS_STATUS = 'SUCCESS'
DTP_PROJECT_ID = os.getenv('dtp_project_id', default=None)
GCLOUD_LOG_NAME = os.getenv('gcloud_log_name',default=None)

BASH_LOG_TEMPLATE=f"""{{
		"dag_id":"$dag_name",
        "workload":"$workload",
		"action":"$action",
		"time":"$time",
		"message":"$message",
		"status":"$status"
    }}"""

def prepare_log_bash_command(gcloud_log_name,dag_name,workload,action,message,status,severity):
    
    gcloud_logging_bash_command = f"""
                gcloud logging write {gcloud_log_name} '{
                    BASH_LOG_TEMPLATE.replace('$dag_name',f'{dag_name}').
                    replace('$workload',f'{workload}').
                    replace('$action',f'{action}').
                    replace('$time',f'{str(datetime.now())}').
                    replace('$message',f'{message}').
                    replace('$status',f'{status}')
                  }' --payload-type=json --severity={severity}
                """
    
    return gcloud_logging_bash_command


def write_execution_logs(context):

    from google.cloud import logging

    exec_time = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    task_id = context['task'].task_id
    dag_name= context['task'].dag_id
    workload=dag_name.split('_')[1]
    table_name = task_id.split('.')[0]
    task_name=task_id.split('.')[-1].replace(f'_{table_name}','')
    job_status=f'{LOG_SUCCESS_STATUS}'
    partition = context.get('task_instance').xcom_pull(task_ids=f'{table_name}.task_start_load_{table_name}')

    message_mapper= {
        'task_gcs_to_bq_load':f'Started - GCS To BQ Load for {table_name} for Partition {partition}',
        'task_bq_table_partition_exists':f'Check If - Table Partition {partition} already exist in table {table_name}',
        'task_df_job_start':f'Started - Dataflow Job - Greenplum to BQ Load for {table_name} for Partition {partition}',
        'task_gp_table_partition_exists':f'Check If - Table Partition {partition} does  exist in table {table_name} for GreenPlum Table'
    }

    action_mapper= {
        'task_gcs_to_bq_load':f'LOAD_GCS_TO_BQ',
        'task_bq_table_partition_exists':f'CHECK_PARTITION',
        'task_df_job_start':f'LOAD_GP_TO_BQ',
        'task_gp_table_partition_exists':f'GP_CHECK_PARTITION'
    }
    

    logging_client = logging.Client(project=DTP_PROJECT_ID)
    logger = logging_client.logger(name=GCLOUD_LOG_NAME)
    logger.log_struct(
        dict(
            {
                "dag_id":dag_name,
                "workload":workload,
                "action":action_mapper[task_name],
                "time":exec_time,
                "message":message_mapper[task_name],
                "status":job_status
            }
        )
    ,severity="INFO")

def write_fail_logs(context):

    from google.cloud import logging
    from re import sub

    exec_time = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    task_id = context['task'].task_id
    dag_name= context['task'].dag_id
    workload=dag_name.split('_')[1]
    table_name = task_id.split('.')[0]
    _task_name = task_id.split('.')[-1].replace(f'_{table_name}','')
    task_name=sub(r'_run_sproc_[0-9]+','',_task_name)
    job_status=f'{LOG_ERROR_STATUS}'
    partition = context.get('task_instance').xcom_pull(task_ids=f'{table_name}.task_start_load_{table_name}')

    message_mapper= {
        'task_gcs_to_bq_load':f'Error-GCS To BQ Load Failed for {table_name} for Partition {partition}',
        'task_bq_table_partition_exists':f'Error-Table Partition {partition} already exist in table {table_name}',
        'task_df_job_start':f'Error-Dataflow Job Failed - Greenplum to BQ Load for {table_name} for Partition {partition}',
        'task_gp_table_partition_exists':f'Error-GreenPlum Table Partition {partition} does not exist in table {table_name}',
        'task_dm_remove_partitions_if_exist': f'Error-Failed to remove partitions from dm tables. Check Airflow Task task_dm_remove_partitions_if_exist for more details',
        'task_check_required_input_hdp_tables_loaded': f'Error-Table Load for All Hadoop Input Tables is Incomplete',
        'task_load_prereq_dm_tables':f'Error-Stored Procedure Run Failed. Check job_metadata.ETL_SQL_LOGS table for more details.',
        'task_final_load':f'Error-Stored Procedure Run Failed. Check job_metadata.ETL_SQL_LOGS table for more details.',
        'task_check_required_input_dm_tables_loaded':f'Error-Table Load for All DM Input Tables is Incomplete.',
        'task_write_etl_sql_logs_to_cloud':f'Error-Failed to write the SQL logs to the job_metadata.ETL_SQL_LOGS table.'

    }

    action_mapper= {
        'task_gcs_to_bq_load':f'LOAD_GCS_TO_BQ',
        'task_bq_table_partition_exists':f'CHECK_PARTITION',
        'task_df_job_start':f'LOAD_GP_TO_BQ',
        'task_gp_table_partition_exists':f'GP_CHECK_PARTITION',
        'task_dm_remove_partitions_if_exist': f'ETL_BQ_REMOVE_PARTITIONS',
        'task_check_required_input_hdp_tables_loaded': f'ETL_BQ_CHECK_INPUT_HDP_TABLE_LOADS',
        'task_load_prereq_dm_tables':f'ETL_RUN_SPROC',
        'task_final_load':f'ETL_RUN_SPROC',
        'task_check_required_input_dm_tables_loaded':f'ETL_BQ_CHECK_INPUT_DM_TABLE_LOADS',
        'task_write_etl_sql_logs_to_cloud':f'ETL_WRITE_LOGS'
    }


    logging_client = logging.Client(project=DTP_PROJECT_ID)
    logger = logging_client.logger(name=GCLOUD_LOG_NAME)
    logger.log_struct(
        dict(
            {
                "dag_id":dag_name,
                "workload":workload,
                "action":action_mapper[task_name],
                "time":exec_time,
                "message":message_mapper[task_name],
                "status":job_status
            }
        )
    ,severity="ERROR")

def write_success_logs(context):

    from google.cloud import logging

    exec_time = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    task_id = context['task'].task_id
    dag_name= context['task'].dag_id
    workload=dag_name.split('_')[1]
    table_name = task_id.split('.')[0]
    task_name=task_id.split('.')[-1].replace(f'_{table_name}','')
    job_status=f'{LOG_SUCCESS_STATUS}'
    partition = context.get('task_instance').xcom_pull(task_ids=f'{table_name}.task_start_load_{table_name}')

    message_mapper= {
        'task_gcs_to_bq_load':f'Success - GCS To BQ Load Success for {table_name} for Partiiton {partition}',
        'task_bq_table_partition_exists':f'Success - Partition {partition} missing in Table {table_name}',
        'task_df_job_start':f'Success - Dataflow Job Success - Greenplum to BQ Load for {table_name} for Partition {partition}',
        'task_gp_table_partition_exists':f'Success - Partition {partition} present in  on-prem  GreenPlum Table {table_name}'
    }

    action_mapper= {
        'task_gcs_to_bq_load':f'LOAD_GCS_TO_BQ',
        'task_bq_table_partition_exists':f'CHECK_PARTITION',
        'task_df_job_start':f'LOAD_GP_TO_BQ',
        'task_gp_table_partition_exists':f'GP_CHECK_PARTITION'
    }

    logging_client = logging.Client(project=DTP_PROJECT_ID)
    logger = logging_client.logger(name=GCLOUD_LOG_NAME)
    logger.log_struct(
        dict(
            {
                "dag_id":dag_name,
                "workload":workload,
                "action":action_mapper[task_name],
                "time":exec_time,
                "message":message_mapper[task_name],
                "status":job_status
            }
        )
    ,severity="INFO")