from unittest import mock
from dfmock import DFMock
from data_processor import injector
from data_processor.dto.step_param import StepParam


def test_filter():

    process_config = "sampleTest"
    input_path = "data_processor/test/resources/sampleTest.txt"
    last_execution_date = '2021-01-27 21:34:34'
    stage_path = 'data_processor/test/resources/'
    dependent_models = ''
    step_config = {
        "name": "DataframeFilter",
        "params": {
            "filter_column": "status_type",
            "filter_value": "ACTIVE"
        }
    }

    mock_spark_session = mock.Mock()
    model_mock = mock.Mock()
    model_mock.fields = mock_spark_session


    step_param = StepParam(input_path, step_config, mock_spark_session, model_mock, dependent_models, process_config,
                           last_execution_date, stage_path)

    columns = { "status_type":"string",
                "hot_dog":"integer",
                "shoelace":"timedelta"
                }
    dfmock = DFMock(count=100, columns=columns)
    dfmock.generate_dataframe()

    my_mocked_dataframe = dfmock.dataframe

    step_param.data_frame = my_mocked_dataframe
    my_mocked_dataframe.filter = mock.Mock()
    injector.get("DataframeFilter").execute(step_param)

    assert 1 == my_mocked_dataframe.filter.call_count
