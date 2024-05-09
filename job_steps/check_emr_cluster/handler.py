import boto3
from settings import Settings
from job_steps import injector
from shared.logging.logger import Logger
from job_steps.check_emr_cluster import STEP_NAME
from job_steps.dto.schedule_state import ScheduleState
from job_steps.dto.emr_cluster import Emr
from shared.utils.datetime import utc_now, date_to_str
from core.entities.cluster_status import ClusterStatus
from shared.utils.custom_json import dumps


def getLivyUrl(ClusterID, client):
    response = client.describe_cluster(
        ClusterId=ClusterID
    )
    master_dns = response['Cluster']['MasterPublicDnsName']
    livy_url = Settings.CLUSTER_DETAILS["livy_details"]["livy_protocol"] + master_dns + \
               Settings.CLUSTER_DETAILS["livy_details"]["livy_port"] + Settings.CLUSTER_DETAILS["livy_details"][
                   "extension"]
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
        # 'STARTING', 'BOOTSTRAPPING',
        response_active = client.list_clusters(
            ClusterStates=[
                ClusterStatus.Running.value, ClusterStatus.Waiting.value, ClusterStatus.Starting.value,
                ClusterStatus.Bootstrapping.value
            ]
        )
        list_of_clusters = response_active['Clusters']
        print(response_active['Clusters'])

        is_cluster_running = False
        is_cluster_starting = False
        for item in list_of_clusters:
            if item['Name'] == Settings.CLUSTER_DETAILS["cluster_name"]:

                logger.info(f'In {STEP_NAME} Status of is_cluster_running is {is_cluster_running}')
                livy_url = getLivyUrl(item['Id'], client)
                cluster_creation_time = date_to_str(item['Status']['Timeline']['CreationDateTime'])
                cluster_id = item['Id']
                cluster_arn = item['ClusterArn']
                cluster_name = item["Name"]
                cluster_status = item['Status']['State']
                if cluster_status in (ClusterStatus.Waiting.value, ClusterStatus.Running.value):
                    is_cluster_running = True
                elif cluster_status in (ClusterStatus.Starting.value, ClusterStatus.Bootstrapping.value):
                    is_cluster_starting = True

                schedule_state.emr_cluster = Emr(cluster_id, cluster_arn, cluster_name, livy_url, cluster_status,
                                                 cluster_creation_time).__dict__
                logger.info(
                    f'SCHEDULE STATE UPDATED :- \n emr_cluster : {schedule_state.emr_cluster}')

        # set cluster information
        if is_cluster_starting:
            schedule_state.check_emr_cluster_choice = "pass"
        elif len(schedule_state.tasks) > 0 and not is_cluster_running:
            schedule_state.check_emr_cluster_choice = "create_cluster"
        elif len(schedule_state.tasks) > 0 and is_cluster_running:
            schedule_state.check_emr_cluster_choice = "schedule_jobs"
        elif len(schedule_state.tasks) == 0 and is_cluster_running and \
                not schedule_state.tasks_in_progress and utc_now().minute > Settings.Schedule["emr_terminate_time"]:
            schedule_state.check_emr_cluster_choice = "remove_cluster"
        elif len(schedule_state.tasks) == 0 and is_cluster_running and \
                schedule_state.tasks_in_progress:
            schedule_state.check_emr_cluster_choice = "pass"
        elif len(schedule_state.tasks) == 0 and not is_cluster_running:
            schedule_state.check_emr_cluster_choice = "pass"
        logger.info(
            f'SCHEDULE STATE UPDATED :- \n check_emr_cluster_choice : {schedule_state.check_emr_cluster_choice}')

        return schedule_state.__dict__
    except Exception as e:
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
        raise e
