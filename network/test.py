#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from network.dbopera import MongoDBOpera
from config.read_cfg import ConfigOp
if __name__ == '__main__':
    cp=ConfigOp()
    ip=cp.get_config('mongdb_cfg','ip')
    port = cp.get_config('mongdb_cfg', 'port')
    print(ip,port)