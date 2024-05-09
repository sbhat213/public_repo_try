from unittest import mock
from data_processor import injector
from data_processor.dto.step_param import StepParam

def test_read_delta_table():

    process_config = "sampleTest"
    input_path = "data_processor/test/resources/sampleTest.txt"
    last_execution_date = '2021-01-27 21:34:34'
    stage_path = 'data_processor/test/resources/'
    dependent_models = ''
    step_config = ''

    mock_spark_session = mock.Mock()

    model_mock = mock.Mock()
    model_mock.path = mock_spark_session
    mock_spark_session.sql = mock.Mock()

    step_param = StepParam(input_path, step_config, mock_spark_session, model_mock, dependent_models, process_config,
                           last_execution_date, stage_path)

    step_param.data_frame = mock.Mock()
    injector.get("ReadDeltaTable").execute(step_param)

    assert 2 == mock_spark_session.sql.call_count

