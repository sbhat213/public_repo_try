from unittest import mock
from dfmock import DFMock
from data_processor import injector
from data_processor.dto.step_param import StepParam

def test_delete_delta_record():

    process_config = "sampleTest"
    input_path = "data_processor/test/resources/sampleTest.txt"
    last_execution_date = '2021-01-27 21:34:34'
    stage_path = 'data_processor/test/resources/'
    dependent_models = ''

    mock_spark_session = mock.Mock()
    model_mock = mock.Mock()
    model_mock.path = mock_spark_session

    mock_spark_session.sql = mock.Mock()
    step_config = {
        "name": "DataframeFilter",
        "params": {
            "filter_column": "status_type",
            "filter_value": "ACTIVE"
        }
    }
    step_param = StepParam(input_path, step_config, mock_spark_session, model_mock, dependent_models, process_config,
                           last_execution_date, stage_path)


    columns = {"super_cool_grouped_column_with_histogram": {"option_count":4, "option_type":"datetime","histogram":(5,3,2,2,)}}

    dfmock = DFMock(count=100, columns=columns)
    dfmock.generate_dataframe()
    step_param.data_frame = dfmock.dataframe
    dfmock.dataframe.withColumn = mock.Mock()
    injector.get("DeleteDeltaRecord").execute(step_param)

    assert 3 == mock_spark_session.sql.call_count
