from enum import Enum


class ClusterStatus(Enum):
    Running = "RUNNING"
    Waiting = "WAITING"
    Starting = 'STARTING'
    Bootstrapping = 'BOOTSTRAPPING'
    Terminating = 'TERMINATING'
    Terminated = 'TERMINATED'
    Terminated_With_Errors = 'TERMINATED_WITH_ERRORS'
