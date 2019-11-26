# -*- coding: utf-8 -*-
#
import sys
import os
import time
import math
import traceback
from datetime import datetime, timezone, timedelta

from .data_getter import DataGetter
from .influx_db import get_intensity_timestamp, store_intensity_data, \
    get_previous_valid_intensity_row
from .config import SITE_NUMBERS_TO_GET_DATA_FOR, SITE_NUMBERS_TO_IGNORE_DATA_FOR, MAXIMUM_LOOKBACK_TIME_DIFF, DEBUG_FILE


class Main(object):
    debug_writer = None

    @classmethod
    def initialize(cls):
        """
        A simple utility method to initalize all basic objects such as
        the debug logger, connect to the mail server and connect to the
        database.
        :return: None
        :rtype: NoneType
        """
        try:
            base_path = os.path.abspath(os.getcwd())
            debug_file = os.path.join(base_path, DEBUG_FILE)
            if not(os.path.exists(debug_file)):
                cls.debug_writer = open(debug_file, "w")
            else:
                cls.debug_writer = open(debug_file, "w+")
            #Decoder.debug_writer = debug_writer
            #FileHandlerMonostate.debug_writer = debug_writer
            #DataExtractor.debug_writer = debug_writer
            millis = math.floor(time.time() * 1000.0)
            cls.debug_writer.write("\n\nSTARTED: " + str(millis) + "\n\n")
        except Exception as e:
            tb_list = traceback.format_tb(e.__traceback__)
            tb_str = "\n".join(tb_list)
            if cls.debug_writer:
                cls.debug_writer.write("Initialization Exception.\n")
                for tb in tb_list:
                    cls.debug_writer.write(tb)
                    cls.debug_writer.write('\n')
            # TODO: mail stack trace tb_str
            raise


    def main(self):
        self.__class__.initialize()

        self.__class__.retrieve_intensity_values()

        try:
            millis = math.floor(time.time() * 1000.0)
            if self.__class__.debug_writer:
                writer = self.__class__.debug_writer
                writer.write("\nFINISHED: " + str(millis) + "\n")
                writer.close()
        except IOError as e:
            pass
        return

    @classmethod
    def retrieve_intensity_values(cls):
        hour_offset = timedelta(hours=1)
        for site_no in SITE_NUMBERS_TO_GET_DATA_FOR:
            database_timestamp = get_intensity_timestamp(site_no)  # type: datetime
            if database_timestamp is None:
                continue
            db_time_millis = database_timestamp.timestamp() * 1000.0
            current_timestamp = datetime.now().astimezone(timezone.utc)
            current_timestamp = current_timestamp.replace(minute=0, second=0, microsecond=0)
            current_time_millis = current_timestamp.timestamp() * 1000.0
            # If we are trying to look back too far: Look back only to MAXIMUM_LOOKBACK_TIME
            if current_time_millis - db_time_millis >= MAXIMUM_LOOKBACK_TIME_DIFF:
                find_time = (current_time_millis - MAXIMUM_LOOKBACK_TIME_DIFF) / 1000.0
                database_timestamp = datetime.utcfromtimestamp(find_time)
                database_timestamp = database_timestamp.replace(tzinfo=timezone.utc)
                db_time_millis = database_timestamp.timestamp() * 1000.0
            # While databaseTimestamp < currentTimestamp
            while (current_time_millis - db_time_millis) >= 0.0:
                data_getter = DataGetter(site_no, database_timestamp)
                intensity = data_getter.get_intensity_from_nmdb()
                if intensity is None:  # No data? Exit.
                    break
                if not cls.is_valid_intensity(intensity):
                    intensity.set_bad_data_flag(1)
                store_intensity_data([intensity])
                database_timestamp = database_timestamp + hour_offset
                # On success, move forward 1 hour.
                db_time_millis = database_timestamp.timestamp() * 1000.0

    @classmethod
    def is_valid_intensity(cls, intensity):
        #        boolean isValid = true;
        is_valid = True
        previous_valid_intensity = get_previous_valid_intensity_row(intensity.site_no, intensity.timestamp)
        current_timestamp_millis = intensity.timestamp.timestamp() * 1000.0
        previous_timestamp_millis = previous_valid_intensity.timestamp.timestamp() * 1000.0
        if not (current_timestamp_millis - previous_timestamp_millis) > MAXIMUM_LOOKBACK_TIME_DIFF:
            current_intensity_val = intensity.intensity_value
            previous_intensity_val = previous_valid_intensity.intensity_value
            if (current_intensity_val < 0.8 * previous_intensity_val) or (current_intensity_val > 1.2 * previous_intensity_val):
                is_valid = False
        return is_valid

def main():
    main = Main()
    main.main()

if __name__ == "__main__":
    main()
