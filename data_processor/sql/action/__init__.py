from injector import singleton

from data_processor import injector
from data_processor.sql.action.concat import Concat
from data_processor.sql.action.current_date import CurrentDate
from data_processor.sql.action.fixed import Fixed
from data_processor.sql.action.job_name import JobName
from data_processor.sql.action.last_execution_date import LastExecutionDate
from data_processor.sql.action.lower import Lower
from data_processor.sql.action.prefix import Prefix
from data_processor.sql.action.regexp_replace import RegexpReplace
from data_processor.sql.action.case import Case
from data_processor.sql.constant import Action
from data_processor.sql.action.date_formatter import DateFormatter
from data_processor.sql.action.timestamp_formatter import TimestampFormatter


injector = injector
injector.binder.bind(Action.FIXED, to=Fixed, scope=singleton)
injector.binder.bind(Action.PREFIX, to=Prefix, scope=singleton)
injector.binder.bind(Action.CONCAT, to=Concat, scope=singleton)
injector.binder.bind(Action.LAST_EXECUTION_DATE, to=LastExecutionDate, scope=singleton)
injector.binder.bind(Action.CURRENT_DATE, to=CurrentDate, scope=singleton)
injector.binder.bind(Action.Regexp_Replace, to=RegexpReplace, scope=singleton)
injector.binder.bind(Action.JOB_NAME, to=JobName, scope=singleton)
injector.binder.bind(Action.LOWER, to=Lower, scope=singleton)
injector.binder.bind(Action.CASE, to=Case, scope=singleton)
injector.binder.bind(Action.DATE_FORMATTER, to=DateFormatter, scope=singleton)
injector.binder.bind(Action.TIMESTAMP_FORMATTER, to=TimestampFormatter, scope=singleton)
