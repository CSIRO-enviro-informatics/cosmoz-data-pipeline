# -*- coding: utf-8 -*-
#
import sys
module = sys.modules[__name__]
CONFIG = module.CONFIG = {}
DEBUG_FILE = "res/debug.log"
SITE_NUMBERS_TO_GET_DATA_FOR = CONFIG['SITE_NUMBERS_TO_GET_DATA_FOR'] = \
    [2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 15, 18, 19, 20, 21, 22, 23]
SITE_NUMBERS_TO_IGNORE_DATA_FOR = CONFIG['SITE_NUMBERS_TO_IGNORE_DATA_FOR'] = \
    [1, 4, 5]
MAXIMUM_LOOKBACK_TIME_DIFF = CONFIG['MAXIMUM_LOOKBACK_TIME_DIFF'] = \
    1000 * 60 * 60 * 24  # 24 Hours


