from injector import Injector

injector = Injector()

from injector import singleton
from data_processor.steps.read.read_csv import ReadCSV

injector.binder.bind('ReadCSV', to=ReadCSV, scope=singleton)
