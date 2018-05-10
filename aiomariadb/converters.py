from decimal import Decimal
from datetime import datetime, date, timedelta

from ._mariadb_async import ffi, lib


# ------- SQL to Python -------


def sql_date_to_py_date(value):
    return date(
        year=int(value[:4]),
        month=int(value[5:7]),
        day=int(value[8:10]))


def sql_time_to_py_timedelta(value):
    h, m, s = value.split(':')
    s, ms = s.split('.') if '.' in s else (s, 0)
    neg = -1 if h[0] == '-' else 1
    return neg * timedelta(
        hours=abs(int(h)),
        minutes=int(m),
        seconds=int(s),
        microseconds=int(ms))


def sql_datetime_to_py_datetime(value):
    if len(value) < 11:
        return sql_date_to_py_date(value)

    dt, ms = value.split('.')
    return datetime(
        int(dt[:4]),
        int(dt[5:7]),
        int(dt[8:10]),
        int(dt[11:13]),
        int(dt[14:16]),
        int(dt[17:19]),
        int(ms))


def sql_set_to_py_set(value):
    # TODO: Implement this!
    pass


def escape_sequence(val, charset, mapping=None):
    n = []
    for item in val:
        quoted = escape_item(item, charset, mapping)
        n.append(quoted)
    return "(" + ",".join(n) + ")"


def sql_null_to_py_none(value):
    return None


def sql_string_to_py_str(value):
    return value.decode('utf8', 'surrogateescape')


sql_to_py_mapping = {
    lib.MYSQL_TYPE_DECIMAL: Decimal,
    lib.MYSQL_TYPE_NEWDECIMAL: Decimal,
    lib.MYSQL_TYPE_TINY: int,
    lib.MYSQL_TYPE_SHORT: int,
    lib.MYSQL_TYPE_LONG: int,
    lib.MYSQL_TYPE_FLOAT: float,
    lib.MYSQL_TYPE_DOUBLE: float,
    lib.MYSQL_TYPE_NULL: sql_null_to_py_none,
    lib.MYSQL_TYPE_TIMESTAMP: sql_datetime_to_py_datetime,
    lib.MYSQL_TYPE_LONGLONG: int,
    lib.MYSQL_TYPE_INT24: int,
    lib.MYSQL_TYPE_DATE: sql_date_to_py_date,
    lib.MYSQL_TYPE_TIME: sql_time_to_py_timedelta,
    lib.MYSQL_TYPE_DATETIME: sql_datetime_to_py_datetime,
    lib.MYSQL_TYPE_YEAR: int,
    lib.MYSQL_TYPE_NEWDATE: sql_date_to_py_date,
    lib.MYSQL_TYPE_VARCHAR: sql_string_to_py_str,
    lib.MYSQL_TYPE_BIT: int,
    lib.MYSQL_TYPE_ENUM: sql_string_to_py_str,
    lib.MYSQL_TYPE_SET: sql_set_to_py_set,
    lib.MYSQL_TYPE_TINY_BLOB: bytearray,
    lib.MYSQL_TYPE_MEDIUM_BLOB: bytearray,
    lib.MYSQL_TYPE_LONG_BLOB: bytearray,
    lib.MYSQL_TYPE_BLOB: bytearray,
    lib.MYSQL_TYPE_VAR_STRING: sql_string_to_py_str,
    lib.MYSQL_TYPE_STRING: sql_string_to_py_str,
}


# ------- Python to SQL -------


def py_date_to_sql_date(value):
    return None


def py_datetime_to_sql_datetime(value):
    return None


def py_timedelta_to_sql_time(value):
    return None


def py_set_to_sql_set(value):
    return None


def py_none_to_sql_null(value):
    return 'NULL'


def py_str_to_sql_string(value):
    return value.encode('utf8', 'surrogateescape')


py_to_sql_mapping = {
    Decimal: py_str_to_sql_string,
    int: py_str_to_sql_string,
    float: py_str_to_sql_string,
    date: py_date_to_sql_date,
    datetime: py_datetime_to_sql_datetime,
    timedelta: py_timedelta_to_sql_time,
    set: py_set_to_sql_set,
    None: py_none_to_sql_null,
    str: py_str_to_sql_string
}
