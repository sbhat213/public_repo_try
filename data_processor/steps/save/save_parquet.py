from data_processor.utils.step_reader import get_overwrite, get_temp


class SaveParquet:

    def execute(self, params):
        overwrite = get_overwrite(params.step_config)
        temp = get_temp(params.step_config)

        path = params.model.path + temp
        if overwrite:
            params.data_frame.write.mode("overwrite").parquet(path)
        else:
            params.data_frame.write.parquet(params.model.path)
