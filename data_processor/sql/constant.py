class Step:
    PARAMS = "params"


class Map:
    FIELD = "field"
    VALUE = "value"
    ACTION = "action"
    COMPARISON = "comparison"
    PARTITION_DATE = "partition_date"
    OPERATOR = "operator"


class Action:
    LOWER = "lower"
    FIXED = "fixed"
    CONCAT = "concat"
    CURRENT_DATE = "current_date"
    PREFIX = "prefix"
    JOB_NAME = "job_name"
    LAST_EXECUTION_DATE = "last_execution_date"
    Regexp_Replace = "regexp_replace"
    CASE = "case"
    DATE_FORMATTER = "date_formatter"
    TIMESTAMP_FORMATTER = "timestamp_formatter"


class Comparison:
    GREATER = "greater"
    LESS = "less"
    NOT = "not"
    NOT_OR_NULL = "not_or_null"


class Operator:
    AND = "and"
    OR = "or"


class Condition:
    FIELD = "field"
    COLUMN = "column"
    BLANK = "blank"
    ELSE = "else"
    IS = "is"
    THEN = "then"


class UDF:
    UUID = "uuid()"
    UUID_NAME = "uuid"
