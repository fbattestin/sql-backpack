from sqlglot import Dialects


def available_dialects():
    dialects_dict = {dialect.name: dialect.value for dialect in Dialects}
    dialects_tuple = tuple(value for value in dialects_dict.values() if value)
    return dialects_tuple
