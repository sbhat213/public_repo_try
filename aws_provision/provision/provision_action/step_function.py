from aws_cdk import (aws_stepfunctions as sfn, aws_stepfunctions_tasks as sfn_tasks, core)


class StepFunction(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

    def create_step_function(self, state_machine_name, definition,role):
        step_function = sfn.StateMachine(
            self, state_machine_name,
            definition=definition,
            timeout=core.Duration.seconds(900),
            state_machine_name=state_machine_name,
            role=role
        )
        return step_function
