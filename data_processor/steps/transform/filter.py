from data_processor.utils.step_reader import get_filter
from settings import Settings


class DataframeFilter:

    def execute(self, params):

        # Fetching filter column and filter value from config file
        column, value = get_filter(params.step_config)

        params.data_frame = params.data_frame.filter(params.data_frame[column] == value)

        # Added for testing
        # if Settings.LOG_FLAG['print_show_count']:
        #     params.data_frame.show(5, False)
