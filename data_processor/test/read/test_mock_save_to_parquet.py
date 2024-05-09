from unittest import mock
import pandas as pd
from data_processor import injector
from data_processor.dto.step_param import StepParam
from shared.dto.model import Model


def test_save_to_parquet():
    process_config = "sampleTest"
    input_path = "data_processor/test/resources/sampleTest.txt"
    last_execution_date = '2021-01-27 21:34:34'
    stage_path = 'data_processor/test/resources/'
    dependent_models = ''
    step_config = ''

    mock_spark_session = mock.Mock()
    temp = {
        "name": "dim_ad",
        "path": "D:/Shared/dim_ad",
        "partition_key": "NA",
        "primary_key": "row_hash",
        "composite_key": ["row_hash"],
        "table_name": "i2dp_tbl_dim_ad",
        "fields": [

        ]
    }

    model = Model(temp)
    step_param = StepParam(input_path, step_config, mock_spark_session, model, dependent_models, process_config,
                           last_execution_date, stage_path)
    step_param.data_frame = pd.DataFrame(
        {'CALL_TRANSACTION_ID': ['a2M4P000000P0vuUAC', 'a2M4P000000P0w0UAC', 'a2M4P000000P0w0PAC'],
         'CALL_RECORD_TYPE': ['ORGCSALL', 'ORGCALYL', 'ORGCALLD']})
    df = mock.Mock()
    step_param.data_frame = df
    dtaframe_write = mock.Mock()

    df.write = dtaframe_write
    df.write.parquet = mock.Mock()
    injector.get("SaveParquet").execute(step_param)

    assert 1 == df.write.parquet.call_count

