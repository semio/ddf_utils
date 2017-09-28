# -*- coding: utf-8 -*-


import logging
import coloredlogs

logger = logging.getLogger('Chef')
coloredlogs.install(logger=logger, fmt='%(asctime)s %(levelname)s %(message)s')
