from data_processor.utils.step_reader import get_overwrite, get_temp
from shared.utils.datetime import utc_now_string
import os
from settings import Settings


class SaveCSV:

    def execute(self, params):
        overwrite = get_overwrite(params.step_config)
        temp = get_temp(params.step_config)
        if temp != "":
            time_fix = utc_now_string().strip().replace(":", "").replace(" ", "-")
            path = os.path.join(params.stage_path, time_fix)
        else:
            path = params.model.path

        if overwrite:
            params.data_frame.write.mode("overwrite") \
                .option("dateFormat", "yyyy-MM-dd")\
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(path,
                                                                      header='true',
                                                                      compression="gzip")
            # Adding for testing purpose
            if Settings.LOG_FLAG['print_show_count']:
                print("Saving latest record to CSV file : " + path)
                print(params.data_frame.show(2, False))
                print("Save_CSV Count : ", params.data_frame.count())
            print("Save_CSV Count : ", params.data_frame.count())
        else:
            params.data_frame.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(params.model.path,
                                                                                         header='true',
                                                                                         compression="gzip")
