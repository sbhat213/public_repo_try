class Settings:
    AWS_CREDENTIALS = {
        "access_key_id": "",
        "secret_access_key": "",
        "region": "ap-south-1",
    }

    DEPLOY_PATH = 'idpmonjuvietljobs/SourceFeeds/1-deploy/'

    # -- Spark Submit property
    # Uncomment below lines of SPARK{} to set spark submit property manually, also uncomment in runner.py file
    SPARK = {
        "file": "s3://idpmonjuvietljobs/SourceFeeds/1-spark/processor.py",
        "pyFiles": ["s3://idpmonjuvietljobs/SourceFeeds/1-spark/spark-job.zip"],
        "jars": ["s3://idpmonjuvietljobs/SourceFeeds/1-spark/drool-jar/*"],

        # "executorCores": 3,
        # "driverCores": 2,
        # "executorMemory": "4096M",
        # "driverMemory": "2048M",
        # "numExecutors": 6,
        # "queue": "default",

        "conf": {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.shuffle.partitions": 10,
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",

            # "spark.dynamicAllocation.enabled": "false",
            # "spark.executor.memoryOverhead": "1024M",
            # "spark.driver.memoryOverhead": "1024M",
            # "spark.memory.fraction": "0.80",
            # "spark.memory.storageFraction": "0.30",
            # "spark.shuffle.compress": "true",
            # "spark.yarn.scheduler.reporterThread.maxFailures": "5"

        }
    }

    SPARK_STEP_FUNCTION = "arn:aws:states:ap-south-1:740277878297:stateMachine:spark_job"
    SPARK_STATUSCHECK_STEP_FUNCTION = "arn:aws:states:ap-south-1:740277878297:stateMachine:check_spark_state"
    Schedule = {
        "limit": 6,
        "retry_count": 3,
        "interval_days": 2,
        "rds_limit": 5,
        "check_limit": 5,
        "month_gap": 30,
        "week_gap": 7,
        "day_gap": 24,
        "cleanup_interval_hours": 0.016,
        "emr_terminate_time": 10,
        "long_running_hours": 6

    }
    s3_staging_files = {
        "bucket": "idpmonjuvietljobs",
        "staging_prefix": "tables_staging/scheduled/{}",
        "processed_prefix": "tables_staging/processed/{}"
    }
    REDSHIFT_CREDENTIALS = {
        "user_name": "idpmorp",
        "password": "(ip%^1*927morP)",
        "host": "monjuviredshiftcluster.cmnf8atl4hzt.ap-south-1.redshift.amazonaws.com",
        "port": "5439",
        "database": "idpmorphosys"
    }

    CLUSTER_DETAILS = {
        "cluster_name": "idp_cluster",
        "livy_details": {"livy_protocol": "http://", "livy_port": ":8998", "extension": "/batches"},
        "livy_conf_params": {"fetch_log_size": '12000', "timeout_check": 'true', 'session_timeout': '2h',
                             'yarn_timeout': '120s', 'session_retain': '12000s', 'livy_log_size': '200000'},
        "cluster_instances": 2,
        "INITIALIZATION_SCRIPT_PATH": "s3://idp-raw-1/emr_script/initialize.sh",
        "LogUri": "s3://idp-raw-1/indegene-poc/",
        'Ec2SubnetId': 'subnet-08776d7b7a7ee1c7d',
        'Ec2KeyName': 'emr',
        "instance_types": {"master": "m5.xlarge", "slave": "m5.xlarge"},
        "emr_retries": 5

    }

    # transformation_partition = shuffle partition for transformation job
    # partition_size = partition size in MB use to calculate no. of partition by dividing total file size
    SHUFFLE_PARTITION = {
        "transformation_partition": 10,
        "partition_size": 2
    }

    # LOG_FLAG : Added to print step execution and sample data in "wait_run_spark" cloudwatch log
    # Set print_show_count = True, to print COUNT AND SHOW BOTH in logs else keep it False
    LOG_FLAG = {
        "print_show_count": False
    }
