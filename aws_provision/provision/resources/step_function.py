from aws_cdk import (aws_stepfunctions as sfn, aws_stepfunctions_tasks as sfn_tasks, core)
from provision.provision_action.step_function import StepFunction
from provision.resources.job_lambda import JobLambda
from config.config_parser import ConfigParser
from step_function_definition.check_spark_state_definition import CheckSparkState
from step_function_definition.update_spark_state_scheduler_definition import UpdateSparkStateScheduler
from step_function_definition.job_scheduler_definition import JobSchedulerDefinition
from step_function_definition.spark_job_definition import SparkJobDefinition


class StepFunctionJob(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

    def createAll(self):
        step_functions_config = StepFunctionJob.step_function_config();
        step_functions_definitions = StepFunctionJob.step_definitions(self)
        for step_function_name in step_functions_config:
            role = self.iam_roles[step_functions_config[step_function_name]['role']]
            StepFunction.create_step_function(self, step_function_name, step_functions_definitions[step_function_name],role)

    def step_definitions(self):
        step_functions_definitions = {}
        step_functions_definitions['job_scheduler'] = JobSchedulerDefinition.definition(self, self.lambda_functions)
        step_functions_definitions['spark_job'] = SparkJobDefinition.definition(self, self.lambda_functions)
        step_functions_definitions['check_spark_state'] = CheckSparkState.definition(self, self.lambda_functions)
        step_functions_definitions['update_spark_state_scheduler'] = UpdateSparkStateScheduler.definition(self,self.lambda_functions)
        return step_functions_definitions

    @staticmethod
    def step_function_config():
        path = f"E:\\Shared\\Indegene\\aws_provision\\config\\"
        steps_functions_config = ConfigParser.parse_config(path + "step_resource.json")
        return steps_functions_config
