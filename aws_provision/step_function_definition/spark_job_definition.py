from aws_cdk import (aws_stepfunctions as sfn, aws_stepfunctions_tasks as sfn_tasks, core)


class SparkJobDefinition:
    def __init__(self):
        pass

    def definition(self, lambda_functions):
        run_spark_job_lambda = lambda_functions['run_spark_job']
        copy_to_redshift_lambda = lambda_functions['copy_to_redshift']

        run_spark_job = sfn_tasks.LambdaInvoke(
            self,
            'step_run_spark_job',
            lambda_function=run_spark_job_lambda,
            payload_response_only=True
        )

        copy_to_redshift = sfn_tasks.LambdaInvoke(
            self,
            'step_copy_to_redshift',
            lambda_function=copy_to_redshift_lambda,
            payload_response_only=True
        )

        spark_job_choice = sfn.Choice(
            self, "spark_job_choice?"
        )

        wrapping_up = sfn.Pass(
            self, "SparkJobDefinition_wrapping_up"
        )

        definition = spark_job_choice.when(sfn.Condition.number_equals("$.task.job_type", 1),
                                           run_spark_job.next(wrapping_up)).when(
            sfn.Condition.number_equals("$.task.job_type", 0), copy_to_redshift.next(wrapping_up))

        return definition
