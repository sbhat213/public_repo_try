from injector import Injector
import sys

injector = Injector()

parent_dir = sys.path[1]
# parent_dir = "D:/Shared/Indegene"
print('parent_dir path : ', parent_dir)


process_config_dir_path = parent_dir + "\\job_config\\process_config"
print("process_config directory name " + process_config_dir_path)

model_dir_path = parent_dir + "\\job_config\\models"
print("model directory name " + model_dir_path)

file_model_dir_path = parent_dir + "\\job_config\\file_model"
print("file_model directory name " + file_model_dir_path)

transform_dir_path = parent_dir + "\\job_config\\transformation_job"
print("transform directory name " + transform_dir_path)


'''
# Set-1
# dim_prescriber  fct_call_activity  dim_product dim_opts  fct_phm_dly_search  fct_pres_email_actvy_stat

# Set-2
# fct_pres_email_sent_dly  fct_call_dtl_msg  dim_pres_opt_xref
# fct_pres_email_opens_dly fct_pres_email_clicks_dly fct_pres_email_bounces_dly

# set-3
# dim_campaign dim_geo_hier  dim_adset fct_pres_opens_clicks_dly  fct_pres_email_send_evnt_dly dim_emp

# set-4
# dim_spkr_programs dim_spkrs  fct_pres_engmt_activity_dtl fct_pres_engmt_activity  fct_pres_rte_dly
'''
