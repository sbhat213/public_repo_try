from core.repository.process_configuration_repository import ProcessConfigurationRepository
from job_steps.copy_to_redshift import STEP_NAME
from job_steps import injector
from shared.dto.step_config import StepConfig
from shared.logging.logger import Logger
from job_steps.copy_to_redshift.copy_data import CopyData
from shared.dto.model import Model
from job_steps.copy_to_redshift.transformation_job_updater import TransformJobUpdater
from core.entities.job_status import JobStatus
from core.repository.model_repository import ModelRepository
from shared.utils.custom_json import map_pynamo_item_to_python
from job_steps.dto.spark_state import SparkState


def lambda_handler(event, context):
    data = event
    spark_state = SparkState()
    spark_state.set_values(data)
    logger = injector.get(Logger)
    injector.get(spark_state.task.task_type).update(spark_state, JobStatus.Processed.value)

    copy_date = injector.get(CopyData)
    model_repository = injector.get(ModelRepository)
    process_config_repository = injector.get(ProcessConfigurationRepository)
    model = Model(map_pynamo_item_to_python(model_repository.get(spark_state.task.model)))
    process_config = StepConfig(
        map_pynamo_item_to_python(process_config_repository.get(spark_state.task.process_config)))
    try:
        copy_date.copy(model, process_config)
        injector.get(spark_state.task.task_type).update(spark_state, JobStatus.Processed.value)
    except Exception as e:
        logger.error(e.args[0])
        raise e

#
# input_redshift_json = {
#     "task": {
#         "task_type": "Transform",
#         "process_config": "fact_call_activity_redshift",
#         "model": "fact_call_activity",
#         "task_id": "fact_call_activity_to_redshift",
#         "url": "",
#         "job_type": 0,
#         "last_success_date": "2021-03-05 13:10:04.796570+00:00",
#         "stage_path": ""
#     },
#     "livyUrl": "http://ip-10-0-1-115.ap-south-1.compute.internal:8998/batches",
#     "response_id": ""
# }
#
# lambda_handler(input_redshift_json, '')
