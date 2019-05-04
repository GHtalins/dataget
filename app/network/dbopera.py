#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import  sys
sys.path.append("/trade/app")
try:
    from app.config.read_cfg import ConfigOp
except Exception as e:
    from config.read_cfg import ConfigOp

from pymongo import MongoClient

import  logging
class MongoDBOpera:
    #初始化，连接MONGODB数据库
    def connect(self,db_section):
        cp=ConfigOp()
        try:
            self._ip,self._port=cp.get_mongocfg(db_section)
            self._conn = MongoClient(self._ip, self._port)
        except Exception as e:
            logging.error(u'__init__ 失败.'+__file__, e)

    def get_collection(self,name,collection):
        _dbname=self._conn[name]
        _collection=_dbname[collection]
        return _collection

    def insert_to_collection(self,collection,query):
        try:
            if collection != None:
                collection.insert(query)
            else:
                logging.info("insert_to_collection 集合句柄不存在，插入数据失败")
        except Exception as e:
            logging.error(u'insert_to_collection ' + collection + ' 失败.'+__file__, e)

    def insert_list_to_collection(self,collection,list):
        try:
            if collection != None:
                if len(list)<140000:
                    collection.insert_many(list)
                else:
                    n=len(list)/140000
                    for i in range(1,n+1):
                        begin=140000*(i-1)
                        end  = 140000*i - 1
                        collection.insert_many(list[begin:end])
                    collection.insert_many(list[140000*n:len(list)-140000*n - 1])
            else:
                logging.info("insert_to_collection 集合句柄不存在，插入数据失败")
        except Exception as e:
            logging.error(u'insert_to_collection ' + collection + ' 失败.'+__file__, e)
    def collection_find_one(self,collection,query):
        try:
            if collection != None:
                _data=collection.find_one(query)
                return _data
            else:
                logging.info("insert_to_collection 集合句柄不存在，插入数据失败")
                return ''
        except Exception as e:
            logging.error(u'find_one query: '+query +'; '+ collection + ' 失败.'+__file__, e)
            return ''

    def collection_find(self,collection,query):
        try:
            _datadict=collection.find(query)
            return _datadict
        except Exception as e:
            logging.error(u'find_all query: '+query +'; '+ collection + ' 失败.'+__file__, e)
            return None
