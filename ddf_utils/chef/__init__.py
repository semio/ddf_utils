# -*- coding: utf-8 -*-


import logging
import coloredlogs

logger = logging.getLogger('Chef')
coloredlogs.install(logger=logger, fmt='%(asctime)s %(name)s %(levelname)s %(message)s')
