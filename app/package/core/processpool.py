#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing
from config.read_cfg import ConfigOp
from multiprocessing import Pool

class ProcessPool(Pool):
    cp = ConfigOp()

    _maxprocess  = int(cp.get_config('process_cfg', 'max_num'))
    _maxpool     = int(cp.get_config('process_cfg', 'max_pool'))
    _curren_pool = 0

    def __init__(self,*args, **kwds):
        #Pool.__init__(self._maxprocess)
        print("1")

    def __new__(cls,*more):
        if cls._curren_pool <cls. _maxpool:
            cls._curren_pool += 1
            return object.__new__(cls,*more)