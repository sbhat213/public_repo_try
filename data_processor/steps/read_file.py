class ReadFile:

    def execute(self, data_frame, config):
        data_frame.write.parquet(config['output_path'], mode='append')
