from enum import Enum


class ErrorCode(Enum):
    bad_response = "BAD RESPONSE"
    rds_timeout = "RDS TIMEOUT"
    long_running_job = "LONG RUNNING JOB"
