import boto3
from job_steps import injector
from settings import Settings
from shared.logging.logger import Logger
from core.entities.cluster_status import ClusterStatus
from job_steps.dto.schedule_state import ScheduleState
from job_steps.dto.emr_cluster import Emr
import time
from job_steps.wait_cluster_creation import STEP_NAME


def getEmrStats(ClusterID, client, schedule_state):
    logger = injector.get(Logger)
    response = client.describe_cluster(
        ClusterId=ClusterID
    )

    cluster_status = response['Cluster']['Status']['State']
    if cluster_status == ClusterStatus.Running.value:

        cluster_creation_time = response['Cluster']['Status']['Timeline']['CreationDateTime'].strftime(
            "%d-%b-%Y (%H:%M:%S.%f)")

        livy_url = Settings.CLUSTER_DETAILS["livy_details"]["livy_protocol"] + response['Cluster'][
            'MasterPublicDnsName'] + Settings.CLUSTER_DETAILS["livy_details"]["livy_port"] + \
                   Settings.CLUSTER_DETAILS["livy_details"]["extension"]
        schedule_state.emr_cluster = Emr(response['Cluster']['Id'], response['Cluster'][''],
                                         response['Cluster']['Name'], livy_url, cluster_creation_time).__dict__
        logger.info(
            f'SCHEDULE STATE UPDATED :- \n emr_cluster : {schedule_state.emr_cluster}')
        return True
    else:
        return False


def lambda_handler(event, context):
    logger = injector.get(Logger)

    try:
        data = event["file_watcher_result"]
        schedule_state = ScheduleState()
        schedule_state.set_values(data)
        client = boto3.client('emr')
        emr_object = schedule_state.emr_cluster
        cluster_id = emr_object['cluster_id']

        number_of_retries = Settings.CLUSTER_DETAILS["emr_retries"]
        while number_of_retries > 0:
            time.sleep(60)
            if getEmrStats(cluster_id, client, schedule_state):
                logger.info('CLUSTER IS RUNNING')
                return schedule_state.__dict__
            number_of_retries = number_of_retries - 1
            logger.info(f'Number of retries in {STEP_NAME} are {number_of_retries}')
        schedule_state.emr_cluster = None
        logger.info(
            f'SCHEDULE STATE UPDATED :- \n emr_cluster : {schedule_state.emr_cluster}')
        return schedule_state.__dict__

    except Exception as e:
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
        raise e
