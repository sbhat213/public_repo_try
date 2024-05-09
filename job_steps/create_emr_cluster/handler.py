import boto3
from job_steps import injector
from shared.logging.logger import Logger
from job_steps.create_emr_cluster import STEP_NAME
from job_steps.dto.schedule_state import ScheduleState
from job_steps.dto.emr_cluster import Emr
from settings import Settings


def lambda_handler(event, context):
    logger = injector.get(Logger)
    try:
        data = event["file_watcher_result"]
        logger.info(f"For {STEP_NAME} Input event is : {event}")
        schedule_state = ScheduleState()
        schedule_state.set_values(data)

        INITIALIZATION_SCRIPT_PATH = Settings.CLUSTER_DETAILS["INITIALIZATION_SCRIPT_PATH"]
        client = boto3.client('emr')
        response = client.run_job_flow(
            Name=Settings.CLUSTER_DETAILS["cluster_name"],
            LogUri=Settings.CLUSTER_DETAILS["LogUri"],
            ReleaseLabel='emr-6.2.0',
            Instances={
                'MasterInstanceType': Settings.CLUSTER_DETAILS["instance_types"]["master"],
                'SlaveInstanceType': Settings.CLUSTER_DETAILS["instance_types"]["slave"],
                'InstanceCount': Settings.CLUSTER_DETAILS["cluster_instances"],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName': Settings.CLUSTER_DETAILS["Ec2KeyName"],
                'Ec2SubnetId': Settings.CLUSTER_DETAILS["Ec2SubnetId"]

            },
            Applications=[{'Name': 'Spark'}, {'Name': 'Livy'}],

            Configurations=[
                {
                    'Classification': 'livy-conf',
                    'Properties': {
                        'livy.server.session.timeout-check': Settings.CLUSTER_DETAILS["livy_conf_params"]["timeout_check"],
                        'livy.server.session.timeout': Settings.CLUSTER_DETAILS["livy_conf_params"]["session_timeout"],
                        'livy.server.yarn.app-lookup-timeout': Settings.CLUSTER_DETAILS["livy_conf_params"]["yarn_timeout"],
                        'livy.server.session.state-retain.sec': Settings.CLUSTER_DETAILS["livy_conf_params"]["session_retain"],
                        'livy.cache-log.size' : Settings.CLUSTER_DETAILS["livy_conf_params"]["livy_log_size"]
                        # 'livy.server.session.timeout-check': 'true',
                        # 'livy.server.session.timeout': '2h',
                        # 'livy.server.yarn.app-lookup-timeout': '120s',
                        # 'livy.server.session.state-retain.sec': '12000s',
                        # 'livy.cache-log.size' : '200000'
                        # 'livy.server.request-header.size' : '13107287',
                        # 'livy.server.response-header.size' : '13107287'

        }
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Steps=[
            ],
            BootstrapActions=[
                {
                    'Name': 'Install packages',
                    'ScriptBootstrapAction': {
                        'Path': INITIALIZATION_SCRIPT_PATH
                    }
                }
            ])
        schedule_state.emr_cluster = Emr(response['JobFlowId'], None, None, None, None, None).__dict__
        logger.info(
            f'SCHEDULE STATE UPDATED :- \n emr_cluster : {schedule_state.emr_cluster}  ')
        return schedule_state.__dict__
    #
    except Exception as e:
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
