import boto3
from settings import Settings
from shared.utils.datetime import date_to_str
from job_steps import injector
from shared.logging.logger import Logger
from job_steps.dto.schedule_state import ScheduleState
from job_steps.dto.emr_cluster import Emr
from job_steps.get_active_emr import STEP_NAME
from core.entities.cluster_status import ClusterStatus


def getLivyUrl(ClusterID, client):
    response = client.describe_cluster(
        ClusterId=ClusterID
    )
    master_dns = response['Cluster']['MasterPublicDnsName']
    livy_url = Settings.CLUSTER_DETAILS["livy_details"]["livy_protocol"] + master_dns + Settings.CLUSTER_DETAILS["livy_details"]["livy_port"] + Settings.CLUSTER_DETAILS["livy_details"]["extension"]
    return livy_url


def lambda_handler(event, context):
    logger = injector.get(Logger)
    logger.info(event)

    try:
        data = event["file_watcher_result"]
        logger.info(f"For {STEP_NAME} Input event is : {event}")
        schedule_state = ScheduleState()
        schedule_state.set_values(data)
        client = boto3.client("emr")
        response_active = client.list_clusters(
            ClusterStates=[
                ClusterStatus.Waiting.value,
            ]
        )
        list_of_clusters = response_active['Clusters']

        for item in list_of_clusters:
            if item['Name'] == Settings.CLUSTER_DETAILS["cluster_name"]:
                logger.error(f"Status of {STEP_NAME} is Active EMR present")
                livy_url = getLivyUrl(item['Id'], client)
                cluster_creation_time = date_to_str(item['Status']['Timeline']['CreationDateTime'])
                cluster_id = item['Id']
                cluster_arn = item['ClusterArn']
                cluster_name = item["Name"]
                cluster_status = item['Status']['State']
                schedule_state.emr_cluster = Emr(cluster_id, cluster_arn, cluster_name, livy_url, cluster_status,
                                                 cluster_creation_time).__dict__
                logger.info(
                    f'SCHEDULE STATE UPDATED :- \n emr_cluster : {schedule_state.emr_cluster}')
        return schedule_state.__dict__

    except Exception as e:
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
        raise e
