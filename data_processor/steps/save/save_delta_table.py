from data_processor.utils.step_reader import get_overwrite
from settings import Settings


class SaveDeltaTable:

    def execute(self, params):
        overwrite = get_overwrite(params.step_config)
        if not params.skip_save:
            if overwrite:
                params.data_frame.write.format("delta").mode("overwrite").save(params.model.path)

                # Added fot testing
                if Settings.LOG_FLAG['print_show_count']:
                    params.data_frame.show(2, False)
                print("SaveDeltaTable Count : ", params.data_frame.count())
            else:
                params.data_frame.write.format("delta").save(params.model.path)
