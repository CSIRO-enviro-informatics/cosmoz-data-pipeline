# -*- coding: utf-8 -*-
#
from datetime import datetime

class Intensity(object):
    __slots__ = ("site_no", "timestamp", "intensity_value", "bad_data_flag")

    def __new__(cls, site_no, timestamp, intensity_value, bad_data_flag=None):
        """
        :param site_no:
        :type site_no: int
        :param timestamp:
        :type timestamp: datetime
        :return:
        """
        self = super(Intensity, cls).__new__(cls)
        self.site_no = site_no

        new_timestamp = datetime(timestamp.year, timestamp.month,
                                 timestamp.day, timestamp.hour,
                                 timestamp.minute, timestamp.second, 0,
                                 tzinfo=timestamp.tzinfo)
        self.timestamp = new_timestamp
        self.intensity_value = intensity_value
        self.bad_data_flag = bad_data_flag
        return self

    def set_bad_data_flag(self, new_bad_data_flag):
        self.bad_data_flag = new_bad_data_flag

    def __str__(self):
        return "Intensity [site_no=" + self.site_no + ", timestamp=" + str(self.timestamp) \
             + ", intensity_value=" + self.intensity_value + ", bad_data_flag=" + self.bad_data_flag + "]"

