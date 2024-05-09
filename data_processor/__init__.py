import pkgutil

from injector import Injector

injector = Injector()

from injector import singleton
from shared.logging.file_logger import FileLogger
from shared.logging.logger import Logger

from data_processor.steps.save.save_delta_table import SaveDeltaTable
from data_processor.steps.save.save_to_redshift import SaveToRedshift
from data_processor.steps.save.save_parquet import SaveParquet
from data_processor.steps.save.save_csv import SaveCSV
from data_processor.steps.read.read_csv import ReadCSV
from data_processor.steps.read.select_data import SelectData
from data_processor.steps.read.read_delta_table import ReadDeltaTable

from data_processor.steps.transform.merge_table import MergeTable
from data_processor.steps.transform.remove_duplicate import RemoveDuplicate
from data_processor.steps.validate.validate import Validate
from data_processor.steps.transform.add_hash import AddHash
from data_processor.steps.validate.validate import Validate
from data_processor.steps.transform.partition_table import PartitionTable
from data_processor.steps.transform.filter import DataframeFilter
from data_processor.steps.transform.add_unique_id import AddUniqueId
from data_processor.steps.transform.delete_delta_record import DeleteDeltaRecord
from data_processor.steps.optimize.cleanUpObsoleteFiles import cleanUpObsoleteFiles
from settings import Settings

injector.binder.bind('ReadCSV', to=ReadCSV, scope=singleton)
injector.binder.bind('ReadDeltaTable', to=ReadDeltaTable, scope=singleton)
injector.binder.bind('MergeTable', to=MergeTable, scope=singleton)
# injector.binder.bind('MergeDataframe', to=MergeDataframe, scope=singleton)
injector.binder.bind('SaveDeltaTable', to=SaveDeltaTable, scope=singleton)
injector.binder.bind('RemoveDuplicate', to=RemoveDuplicate, scope=singleton)
injector.binder.bind('Validate', to=Validate, scope=singleton)
injector.binder.bind('AddHash', to=AddHash, scope=singleton)
injector.binder.bind('SelectData', to=SelectData, scope=singleton)
injector.binder.bind('SaveToRedshift', to=SaveToRedshift, scope=singleton)
injector.binder.bind('ValidateData', to=Validate, scope=singleton)
injector.binder.bind('SaveParquet', to=SaveParquet, scope=singleton)
injector.binder.bind('SaveCSV', to=SaveCSV, scope=singleton)
injector.binder.bind('PartitionTable', to=PartitionTable, scope=singleton)
injector.binder.bind('DataframeFilter', to=DataframeFilter, scope=singleton)
injector.binder.bind('AddUniqueId', to=AddUniqueId, scope=singleton)
injector.binder.bind('DeleteDeltaRecord', to=DeleteDeltaRecord, scope=singleton)
injector.binder.bind('cleanUpObsoleteFiles', to=cleanUpObsoleteFiles, scope=singleton)

STEP_NAME = 'spark_job'

injector.binder.bind(Logger, to=FileLogger, scope=singleton)
