#!/usr/bin/python3m
# -*- coding: utf8 -*-

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from optparse import OptionParser

from db2bkp.DB2Backup import DB2Backup


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="config",
                      help="configuration file", metavar="FILE")
    (options, args) = parser.parse_args()
    if not options.config:
        parser.error('no configuration file')

    with DB2Backup(options.config, options) as bkp:
        bkp.backup()