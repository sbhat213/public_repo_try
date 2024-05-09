from enum import Enum


class FileStatus(Enum):
    File_Submitted = 0
    In_Progress = 1
    Processed = 2
    Failed = 3
