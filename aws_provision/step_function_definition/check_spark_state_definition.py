from aws_cdk import (aws_stepfunctions as sfn, aws_stepfunctions_tasks as sfn_tasks, core)


class CheckSparkState:
    def __init__(self):
        pass

    def definition(self, lambda_functions):
        wait_run_spark_job_lambda = lambda_functions['wait_run_spark_job']
        send_failure_notification_lambda = lambda_functions['send_failure_notification']

        wait_run_spark_job = sfn_tasks.LambdaInvoke(
            self,
            'step_wait_run_spark_job',
            lambda_function=wait_run_spark_job_lambda,
            payload_response_only=True
        )

        send_failure_notification = sfn_tasks.LambdaInvoke(
            self,
            'step_send_failure_notification',
            lambda_function=send_failure_notification_lambda,
            payload_response_only=True
        )

        wait_run_spark_job_choice = sfn.Choice(
            self, "wait_run_spark_job_choice?"
        )

        wrapping_up = sfn.Pass(
            self, "CheckSparkState_wrapping_up"
        )

        definition = wait_run_spark_job.next(
            wait_run_spark_job_choice.when(sfn.Condition.number_equals("$.status", 3),
                                           send_failure_notification.next(wrapping_up)).otherwise(wrapping_up))

        return definition
