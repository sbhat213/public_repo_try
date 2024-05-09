from enum import Enum


class JobStatus(Enum):
    Created = 0
    In_Progress = 1
    Processed = 2
    Failed = 3
    Ready_To_Schedule = 4
