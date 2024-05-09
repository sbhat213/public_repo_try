from aws_cdk import (aws_stepfunctions as sfn, aws_stepfunctions_tasks as sfn_tasks, core)


class JobSchedulerDefinition:
    def __init__(self):
        pass

    def definition(self, lambda_functions):
        create_emr_lambda = lambda_functions['create_emr_cluster']
        terminate_emr_lambda = lambda_functions['remove_emr_cluster']
        check_available_files_lambda = lambda_functions['check_available_files']
        check_emr_cluster_lambda = lambda_functions['check_emr_cluster']
        schedule_jobs_lambda = lambda_functions['schedule_job']
        get_emr_status_lambda = lambda_functions['get_active_emr']

        check_available_files = sfn_tasks.LambdaInvoke(
            self,
            'step_check_available_files',
            lambda_function=check_available_files_lambda,
            result_path="$.file_watcher_result",
            payload_response_only=True
        )

        check_emr_cluster = sfn_tasks.LambdaInvoke(
            self,
            'step_check_emr_cluster',
            lambda_function=check_emr_cluster_lambda,
            result_path="$.file_watcher_result",
            payload_response_only=True
        )

        create_emr = sfn_tasks.LambdaInvoke(
            self,
            'step_create_emr',
            lambda_function=create_emr_lambda,
            result_path="$.file_watcher_result",
            payload_response_only=True
        )

        terminate_emr = sfn_tasks.LambdaInvoke(
            self,
            'step_terminate_emr',
            lambda_function=terminate_emr_lambda,
            payload_response_only=True
        )

        schedule_jobs = sfn_tasks.LambdaInvoke(
            self,
            'step_schedule_jobs',
            lambda_function=schedule_jobs_lambda,
            payload_response_only=True
        )

        get_emr_status = sfn_tasks.LambdaInvoke(
            self,
            'step_get_emr_status',
            lambda_function=get_emr_status_lambda,
            payload_response_only=True
        )

        check_emr_cluster_choice = sfn.Choice(
            self, "check_emr_cluster_choice?"
        )
        should_schedule_jobs = sfn.Choice(
            self, "should_schedule_jobs?"
        )
        wrapping_up = sfn.Pass(
            self, "JobSchedulerDefinition_wrapping_up"
        )
        wait_15_minutes = sfn.Wait(
            self, "wait_15_minutes", time=sfn.WaitTime.duration(core.Duration.minutes(600))
        )

        definition = check_available_files \
            .next(check_emr_cluster) \
            .next(check_emr_cluster_choice
                  .when(sfn.Condition.string_equals("$.file_watcher_result.check_emr_cluster_choice", "create_cluster"),
                        create_emr.next(wait_15_minutes.next(get_emr_status.next(should_schedule_jobs
                            .when(sfn.Condition.is_null("$.file_watcher_result.emr_cluster"), wrapping_up)
                            .when(sfn.Condition.is_not_null("$.file_watcher_result.emr_cluster"),schedule_jobs.next(wrapping_up))
                        ))))
                  .when(sfn.Condition.string_equals("$.file_watcher_result.check_emr_cluster_choice", "remove_cluster"),terminate_emr.next(wrapping_up))
                  .when(sfn.Condition.string_equals("$.file_watcher_result.check_emr_cluster_choice","schedule_jobs"), schedule_jobs)
                  .when(sfn.Condition.string_equals("$.file_watcher_result.check_emr_cluster_choice", "pass"),wrapping_up)
                  .otherwise(wrapping_up)
                  )

        return definition
