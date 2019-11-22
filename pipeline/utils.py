# -*- coding: utf-8 -*-
#
"""utils.py"""
from datetime import datetime, date, timedelta

def do_load_dotenv():
    if do_load_dotenv.completed:
        return True
    from dotenv import load_dotenv
    load_dotenv()
    do_load_dotenv.completed = True
    return True
do_load_dotenv.completed = False


def isostring_to_datetime(iso_string):
    """
    :param iso_string:
    :type iso_string: str
    :return: the python datetime obj
    :rtype: datetime
    """
    if iso_string.endswith('Z'):
        iso_string = iso_string[:-1]+"+0000"
    else:
        last4 = iso_string[-4:]
        if ":" in last4:
            # something like +04:30, change to 0430
            iso_string = iso_string[:-4] + last4.replace(":", "")
    try:
       return datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%S%z")
    except:
       return datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%S.%f%z")

delta_1h = timedelta(hours=1)
delta_1m = timedelta(minutes=1)
delta_1s = timedelta(seconds=1)
def datetime_to_isostring(py_datetime):
    """
    :param py_datetime:
    :type py_datetime: datetime
    :return: the iso string representation
    :rtype: str
    """
    tz = getattr(py_datetime, "tzinfo", None)  # type: tzinfo
    if tz:
        off = tz.utcoffset(py_datetime)
        if off is not None:
            if off.days < 0:
                sign = "-"
                off = -off
            else:
                sign = "+"
            hh, mm = divmod(off, delta_1h)
            mm, ss = divmod(mm, delta_1m)
            if ss >= delta_1s:
                raise RuntimeError("ISO Datetime string cannot have UTC offset with seconds component")
            if hh == 0 and mm == 0:
                suffix = "Z"
            else:
                suffix = "%s%02d%02d" % (sign, hh, mm)
        else:
            suffix = "Z"
    else:
        suffix = "Z"

    if isinstance(py_datetime, date) and not isinstance(py_datetime, datetime):
        datetime_string = py_datetime.strftime("%Y-%m-%dT00:00:00")
    else:
        micros = getattr(py_datetime, 'microsecond', 0)
        if micros > 0:
            datetime_string = py_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")
        else:
            datetime_string = py_datetime.strftime("%Y-%m-%dT%H:%M:%S")
    return datetime_string + suffix


def sql_to_isostring(sql_datetime):
    """
    Assumes sql date string is in UTC
    :param sql_datetime:
    :return:
    """
    timestamp_parts = str(sql_datetime).split(' ')
    return "{}T{}Z".format(timestamp_parts[0], timestamp_parts[1])


def datetime_to_sql(py_datetime):
    """
    Assumes datetime is in UTC.
    :param py_datetime:
    :type py_datetime: datetime
    :return: the sql string representation
    :rtype: str
    """
    return py_datetime.strftime("%Y-%m-%d %H:%M:%S")
