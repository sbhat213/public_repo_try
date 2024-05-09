class Settings:
    LIVY_URL = 'http://' + 'localhost' + ':8998'
    AWS_CREDENTIALS = {
        "access_key_id": "",
        "secret_access_key": "",
        "region": "ap-south-1",
    }
    DEPLOY_PATH = 'idpmonjuvietljobs/SourceFeeds/1-deploy/'

    SPARK = {
        "file": "local:/home/jovyan/work/Indegene/job_processor/processor.py",
        "pyFiles": ["local:/home/jovyan/work/build/spark-job/spark-job.zip",
                    "local:/home/jovyan/work/build/python-dependency/python-dependency.zip"]
    }
    SPARK_STEP_FUNCTION = ""
    Schedule = {
        "limit": 2,
        "retry_count": 3,
        "interval_days": 2
    }
