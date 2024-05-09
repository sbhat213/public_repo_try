from aws_cdk import (aws_stepfunctions as sfn, aws_stepfunctions_tasks as sfn_tasks, core)


class UpdateSparkStateScheduler:
    def __init__(self):
        pass

    def definition(self, lambda_functions):
        check_in_progress_files_lambda = lambda_functions['check_inprogress_files']
        check_scheduled_jobs_lambda = lambda_functions['check_scheduled_jobs']

        check_in_progress_files = sfn_tasks.LambdaInvoke(
            self,
            'step_check_in_progress_files',
            lambda_function=check_in_progress_files_lambda,
            result_path="$.in_progress_watcher_result",
            payload_response_only=True
        )

        check_scheduled_jobs = sfn_tasks.LambdaInvoke(
            self,
            'step_check_scheduled_jobs',
            lambda_function=check_scheduled_jobs_lambda,
            result_path="$.in_progress_watcher_result",
            payload_response_only=True
        )

        wrapping_up = sfn.Pass(
            self, "UpdateSparkStateScheduler_wrapping_up"
        )

        definition = check_in_progress_files.next(check_scheduled_jobs.next(wrapping_up))

        return definition
