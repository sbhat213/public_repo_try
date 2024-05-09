from injector import inject

from core.repository.model_repository import ModelRepository
from core.repository.process_configuration_repository import ProcessConfigurationRepository
from shared.dto.model import Model
from shared.dto.step_config import StepConfig
from shared.utils.custom_json import map_pynamo_item_to_python
from shared.redshift.redshift_sql_client import RedshiftSQLClient


class TableMigrator:

    @inject
    def __init__(self, sql_client: RedshiftSQLClient, model_repository: ModelRepository,
                 config_repository: ProcessConfigurationRepository):
        self.model_repository = model_repository
        self.config_repository = config_repository
        self.list = [
            {
                "model": "customer_master",
                "alias": "i2dp_tbl_dim_prescriber_v2"
            },
            # {
            #     "model": "fct_pres_engmt_activity_dtl",
            #     "alias": "i2dp_tbl_fct_pres_engmt_activity_dtl_v2"
            # }
            # ,
            # {
            #     "model": "dim_emp",
            #     "alias": "i2dp_tbl_dim_emp_v2"
            # },
            # {
            #     "model": "dim_geo_hier",
            #     "alias": "i2dp_tbl_dim_geo_hier_v2"
            # },
            # {
            #     "model": "dim_opts",
            #     "alias": "i2dp_tbl_dim_opts_v2"
            # },
            # {
            #     "model": "dim_pres_opt_xref",
            #     "alias": "i2dp_tbl_dim_pres_opt_xref_v2"
            # },
            # {
            #     "model": "dim_product",
            #     "alias": "i2dp_tbl_dim_product_v2"
            # },
            # {
            #     "model": "fct_pres_engmt_activity_dtl",
            #     "alias": "i2dp_tbl_fct_pres_engmt_activity_dtl_v2"
            # },
            # {
            #     "model": "dim_pres_opt_xref",
            #     "alias": "i2dp_tbl_fct_call_activity_v2_staging"
            # },
            # {
            #     "model": "dim_spkrs",
            #     "alias": "i2dp_tbl_dim_spkrs_v2"
            # }
            # ,
            # {
            #     "model": "fact_call_activity",
            #     "alias": "i2dp_tbl_fct_call_activity_v2"
            # },
            # {
            #     "model": "fct_call_dtl_msg",
            #     "alias": "i2dp_tbl_fct_call_dtl_msg_v2"
            # },
            # {
            #     "model": "fct_phm_dly_search",
            #     "alias": "i2dp_tbl_fct_phm_dly_search_v2"
            # },
            # {
            #     "model": "fct_pres_email_actvy_stat",
            #     "alias": "i2dp_tbl_fct_pres_email_actvy_stat_v2"
            # },
            # {
            #     "model": "fct_pres_email_bounces_dly",
            #     "alias": "i2dp_tbl_fct_pres_email_bounces_dly_v2"
            # },
            # {
            #     "model": "fct_pres_email_clicks_dly",
            #     "alias": "i2dp_tbl_fct_pres_email_clicks_dly_v2"
            # },
            # {
            #     "model": "fct_pres_email_opens_dly",
            #     "alias": "i2dp_tbl_fct_pres_email_opens_dly_v2"
            # },
            #
            # {
            #     "model": "fct_pres_email_send_evnt_dly",
            #     "alias": "i2dp_tbl_fct_pres_email_send_evnt_dly_v2"
            # },
            # {
            #     "model": "fct_pres_email_sent_dly",
            #     "alias": "i2dp_tbl_fct_pres_email_sent_dly_v2"
            # },
            # {
            #     "model": "fct_pres_engmt_activity",
            #     "alias": "i2dp_tbl_fct_pres_engmt_activity_v2"
            # },
            # {
            #     "model": "fct_pres_opens_clicks_dly",
            #     "alias": "i2dp_tbl_fct_pres_opens_clicks_dly_v2"
            # },
            # {
            #     "model": "fct_pres_rte_dly",
            #     "alias": "i2dp_tbl_fct_pres_rte_dly_v2"
            # },
            # {
            #     "model": "dim_spkr_programs",
            #     "alias": "i2dp_tbl_dim_spkr_programs_v2"
            # },
            # {
            #     "model": "dim_ad",
            #     "alias": "i2dp_tbl_dim_ad_v2"
            # },
            # {
            #     "model": "dim_adset",
            #     "alias": "i2dp_tbl_dim_adset_v2"
            # },
            # {
            #     "model": "dim_campaign",
            #     "alias": "i2dp_tbl_dim_campaign_v2"
            # }
        ]
        self.sql_client = sql_client

    def migrate_schema(self, truncate=False):
        for item in self.list:
            model = item["model"]
            print("Using Model : " + str(model))
            table_name = item["alias"]
            model = Model(map_pynamo_item_to_python(self.model_repository.get(model)))
            table_fields = []
            for field in model.fields:
                table_fields.append(self.get_field_type(field))

            fields_query = " ,".join(table_fields)
            query = f"CREATE TABLE IF NOT EXISTS  {table_name} ({fields_query});"
            self.sql_client.execute_query(query)
            print("Table Created : " + table_name)

    def get_field_type(self, field):
        name = field.name
        field_type = field.type
        length = f"({field.length})"
        if field_type == "datetime":
            field_type = "TIMESTAMP"
        if field_type == "long":
            field_type = "bigint"
        if field_type in ["bigint", "long", "date", "TIMESTAMP", "timestamp"]:
            length = ""
        return f"{name}   {field_type}{length}"
