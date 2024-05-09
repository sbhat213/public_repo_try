from injector import inject
from settings import Settings
from core.repository.model_repository import ModelRepository
from shared.dto.model import Model
from shared.utils.custom_json import map_pynamo_item_to_python


class cleanUpObsoleteFiles:

    @inject
    def __init__(self, model_repository: ModelRepository):
        self.model_repository = model_repository

    def execute(self, params):
        print(f"modellist --> {params.step_config['params']['modellist']}")
        modellist = params.step_config['params']['modellist']
        for ele in modellist:
            print(ele)
            model = Model(map_pynamo_item_to_python(self.model_repository.get(ele)))
            deltaPath = model.path
            cleanup_interval_hours = Settings.Schedule["cleanup_interval_hours"]
            query = f"VACUUM '{deltaPath}' RETAIN {cleanup_interval_hours} HOURS  "
            query_2 = "VACUUM {} RETAIN {} HOURS  ".format("'" + str(deltaPath) + "'",cleanup_interval_hours)
            print(f"query : {query_2}")
            df = params.spark.sql("VACUUM {} RETAIN {} HOURS  ".format("'" + str(deltaPath) + "'",cleanup_interval_hours))
            print(f"Below are the unreferenced files that will be deleted --- ")
            df.show(20, False)
            df = params.spark.sql("DESCRIBE HISTORY {}".format("'" + str(deltaPath) + "'"))
            df.show(200, False)
