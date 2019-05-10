#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from lxml import etree
import logging
from app.package.data.logger import log

class WebFormat:

    #校验&获取JSON格式
    def check_json(self,data):
        try:

            _json_data = json.loads(data)
            return _json_data
        except Exception as e:
            log.logger.error(u'check_json 转换失败.' + __file__, e)
            log.logger.info(u"data is["+data+"]")
            return ''

    #校验&获取HTML格式
    def check_html(self,data):
        try:
            _html_data=etree.HTML(data)
            return _html_data
        except Exception as e:
            log.logger.error(u'check_html 转换失败.' + __file__, e)
            return ''

    #将bytes转成字符串
    def bytedecode(self,data):
        try:
            _str_data=bytes.decode(data)
            return _str_data
        except Exception as e:
            log.logger.error(u'bytedecode 转换失败.' + __file__, e)
            return ''
