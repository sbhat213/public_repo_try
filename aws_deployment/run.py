from shared.configuration.config_parser import ConfigParser
from aws_deployment.deployment import Deployment

dep = Deployment()
path = f"D:\\Shared\\Indegene\\aws_deployment\\deploy\\config\\"
pipeline = ConfigParser.parse_config(path + "save_source_file.json")
# pipeline_2 = ConfigParser.parse_config(path + "function_dependency.json")
pipeline_3 = ConfigParser.parse_config(path + "check_available_files.json")
pipeline_5 = ConfigParser.parse_config(path + "check_emr_cluster.json")
pipeline_6 = ConfigParser.parse_config(path + "create_emr_cluster.json")
pipeline_7 = ConfigParser.parse_config(path + "remove_emr_cluster.json")
pipeline_8 = ConfigParser.parse_config(path + "schedule_job.json")
pipeline_9 = ConfigParser.parse_config(path + "wait_cluster_creation.json")
pipeline_10 = ConfigParser.parse_config(path + "get_active_emr.json")
pipeline_11 = ConfigParser.parse_config(path + "run_spark_job.json")
pipeline_12 = ConfigParser.parse_config(path + "wait_run_spark_job.json")
pipeline_13 = ConfigParser.parse_config(path + "copy_to_redshift.json")
pipeline_14 = ConfigParser.parse_config(path + "spark_job.json")
pipeline_15 = ConfigParser.parse_config(path + "check_inprogress_files.json")
pipeline_16 = ConfigParser.parse_config(path + "check_scheduled_jobs.json")
pipeline_17 = ConfigParser.parse_config(path + "send_failure_notification.json")
pipeline_18 = ConfigParser.parse_config(path + "kill_long_running_jobs.json")


list = []

list.append(pipeline)
# list.append(pipeline_2)
list.append(pipeline_3)
list.append(pipeline_5)
list.append(pipeline_6)
list.append(pipeline_7)
list.append(pipeline_8)
list.append(pipeline_9)
list.append(pipeline_10)
list.append(pipeline_11)
list.append(pipeline_12)
list.append(pipeline_13)
list.append(pipeline_14)
list.append(pipeline_15)
list.append(pipeline_16)
list.append(pipeline_17)
list.append(pipeline_18)

for item in list:
    dep.deploy(item)
