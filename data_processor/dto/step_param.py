class StepParam:

    def __init__(self, input_path, step_config, spark, model, dependent_models, process_config,
                 last_execution_date=None, stage_path=""):
        self.input_path = input_path
        self.step_config = step_config
        self.spark = spark
        self.model = model
        self.dependent_models = dependent_models
        self.process_config = process_config
        self.skip_save = False
        self.data_frame = None
        self.last_execution_date = last_execution_date
        self.stage_path = stage_path
