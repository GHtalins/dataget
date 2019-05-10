#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

from urllib import request
try:
    from app.package.format.webformat import WebFormat
    from app.package.data.logger import Logger
    from app.package.data.logger import log
except Exception as e:
    from ...package.format import WebFormat
    from ...package.data.logger import Logger
import logging

#log = Logger('dataget.log',level='debug')
class WebOpera():
    def __init__(self,url):
        self._url=url
        self.set_webheader()

    #设置请求头
    def set_webheader(self):
        self._req = request.Request(self._url)
        self._req.add_header("Accept", "application/json, text/plain, */*")
        self._req.add_header("User-Agent",
                        "User-Agent:Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
                        " Chrome/68.0.3440.106 Safari/537.36")

    # 访问网页，获取数据返回
    def get_web_response(self):
        self._response=''
        try:
            self._response=request.urlopen(self._req).read()
            if type(self._response) == bytes:
                self._response=WebFormat().bytedecode(self._response)
            self._response = WebFormat().check_json(self._response)
            if self._response == "":
                log.logger.warning("获取请求数据失败")
            return self._response
        except Exception as e:
            log.logger.error(u'get_web_response url'+self._url+' 失败.'+__file__, e)
            return ''

    def get_web_html(self):
        self._response = ''
        try:
            self._response = request.urlopen(self._req).read()
            if type(self._response) == bytes:
                self._response=WebFormat().bytedecode(self._response)
            return self._response
        except Exception as e:
            log.logger.error(u'get_web_response url' + self._url + ' 失败.' + __file__, e)
            return ''



