import boto3
from job_steps import injector
from job_steps.dto.schedule_state import ScheduleState
from shared.logging.logger import Logger
from job_steps.remove_emr_cluster import  STEP_NAME



def lambda_handler(event, context):
    logger = injector.get(Logger)

    try:

        client = boto3.client("emr")
        data = event["file_watcher_result"]
        logger.info(f"For {STEP_NAME} Input event is : {event}")


        schedule_state = ScheduleState()
        schedule_state.set_values(data)
        cluster_id = schedule_state.emr_cluster.cluster_id

        logger.info(f"TERMINATE REQUEST RECEIVED FOR CLUSTER {cluster_id}")
        client.set_termination_protection(JobFlowIds=cluster_id, TerminationProtected=False)
        response = client.terminate_job_flows(JobFlowIds=cluster_id)
        #return schedule_state.__dict__




    except Exception as e:
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")

