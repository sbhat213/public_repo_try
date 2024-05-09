from data_processor.sql.constant import Step


def get_add_to_source(step):
    if Step.PARAMS in step and 'add_to_source' in step[Step.PARAMS]:
        return step[Step.PARAMS]["add_to_source"]
    else:
        return None


def get_overwrite(step):
    if Step.PARAMS in step and 'overwrite' in step[Step.PARAMS]:
        return step[Step.PARAMS]["overwrite"]
    else:
        return False


def get_filter(step):
    if Step.PARAMS in step and 'filter_column' in step[Step.PARAMS]:
        return step[Step.PARAMS]["filter_column"], step[Step.PARAMS]["filter_value"]
    else:
        return False


def get_temp(step):
    if Step.PARAMS in step and 'to' in step[Step.PARAMS]:
        return step[Step.PARAMS]["to"]
    else:
        return ""


def get_join_map(step):
    return step[Step.PARAMS]["join"]


def get_join_type(step):
    return step[Step.PARAMS]["join"]["type"]


def get_join_with(step):
    return step["with"]


def get_separator(step):
    if Step.PARAMS in step and 'separator' in step[Step.PARAMS]:
        return step[Step.PARAMS]["separator"]
    else:
        return ','


def get_join_field(step):
    if 'join_field' in step[Step.PARAMS]:
        return step[Step.PARAMS]["join_field"]
    else:
        return ''


def get_skip_global_map(step):
    if 'skip_global_core_map' in step[Step.PARAMS]:
        return step[Step.PARAMS]["skip_global_core_map"]
    else:
        return False


def get_skip_insert(step):
    if 'skip_insert' in step[Step.PARAMS]:
        return step[Step.PARAMS]["skip_insert"]
    else:
        return False


def get_skip_update(step):
    if 'skip_update' in step[Step.PARAMS]:
        return step[Step.PARAMS]["skip_update"]
    else:
        return False


def get_depends_on(step):
    return step["depends_on"]


def get_case_conditions(step):
    return step["conditions"]


def get_step_map(step):
    if Step.PARAMS in step and "map" in step[Step.PARAMS]:
        return step[Step.PARAMS]["map"]
    else:
        return {}


def get_filter_map(step):
    return step[Step.PARAMS]["filter"]


def get_where_map(step):
    if Step.PARAMS in step and "where" in step[Step.PARAMS]:
        return step[Step.PARAMS]["where"]
    else:
        return ""


def get_duplicate_map(step):
    if Step.PARAMS in step and "duplicate" in step[Step.PARAMS]:
        return step[Step.PARAMS]["duplicate"]
    else:
        return ""


def get_matched_map(step):
    if 'matched' in step[Step.PARAMS]:
        return step[Step.PARAMS]["matched"]
    else:
        return {}


def get_input_path(step):
    return step["input_path"]


def get_primary_key(step):
    return step["primary_key"]


def get_output_path(step):
    return step["path"]


def get_table_name(step):
    return step["table_name"]


def get_partition_key(step):
    return step[Step.PARAMS]["key"]


def get_partition_action(step):
    return step[Step.PARAMS]["action"]
