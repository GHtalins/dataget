#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pymongo import MongoClient
import json
import time
from lxml import etree
from concurrent.futures import ThreadPoolExecutor, as_completed



conn = MongoClient('localhost', 27017)
mydb = conn.local
success_collect = mydb.success_Collection
fail_collect=mydb.fail_Collection
code_collect=mydb.code_Collection






#获取持币数据
def get_holds_info(code):
    url="https://dncapi.bqiapp.com/api/coin/holders?code="+str(code)+"&side=30&webp=1"
    try:
        response_data=get_web_response(url)
        hold_info=check_holds_json(response_data)["data"]
        holds_store_info={'total_holders':hold_info["total_holders"],
                                  'percentage_top10':hold_info["percentage_top10"],
                                  'percentage_top20': hold_info["percentage_top20"],
                                  'percentage_top50': hold_info["percentage_top50"],
                                  'update_time': hold_info["update_time"],
                                  'holders_list':hold_info["holders_list"]}
    except Exception as e:
        print(u'get_holds_info获取货币'+code+'持币数据失败', e)
        return ''
    return holds_store_info

def check_holds_json(data):
    try:
        holds_json = json.loads(data)
    except Exception as e:
        print(u'check_holds_info解析持币JSON数据失败', e)
        return ''
    return holds_json

#获取币种描述
def get_desc_info(code):
    desc_url="https://www.feixiaohao.com/currencies/"+str(code)
    try:
        desc_data=bytes.decode(get_web_response(desc_url))
        infos = etree.HTML(desc_data)
        desc_info = infos.xpath("//div[@class='coinIntroduce']/div[@class='textBox']")
        fxsj_info = infos.xpath("//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[0]
        fxjg_info = infos.xpath( "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[1]
        zdgy_info = infos.xpath( "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[2]
        zgy_info = infos.xpath( "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[3]
        jys_info = infos.xpath( "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[4]
        sf_info = infos.xpath( "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[5]
        po_info = infos.xpath( "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[6]
    except Exception as e:
        print(u'get_desc_info获取货币'+code+'描述数据失败', e)
        return ''
    return {"描述":desc_info[0].text,"发行时间":fxsj_info,"发行价格":fxjg_info,"最大供应":zdgy_info,"总供应":zgy_info,"上架交易所":jys_info,"算法":sf_info,"激励机制":po_info}

#获取币种列表
def get_coin_list(page):
    page_url="https://dncapi.bqiapp.com/api/coin/web-coinrank?page="+str(page)+"&type=0&pagesize=100&webp=1"
    try:
        coin_list=get_web_response(page_url)
        coin_list=bytes.decode(coin_list)
    except Exception as e:
        print(u'get_coin_list获取货币列表页'+str(page)+'数据失败', e)
        return ''
    return json.loads(coin_list)

#执行货币详细信息抓取
def run_info(page):
    # 创建一个字典，用于存储当前币种信息
    coin_info = {}
    fail_info = {}
    try:
        code_list=code_collect.find({'page':page})
        for obj in code_list:
            ticks = time.time()
            coin_info.update({'_id':(str(ticks)+str(obj["code"])),'code': obj["code"], 'name': obj["name"],'exec_time':localDate})
            # 根据币种code去取得实际持币url，获取持币信息
            coin_hold_info = get_holds_info(obj["code"])
            # 根据币种code，去取得实际描述url,获取描述信息
            coin_desc_info = get_desc_info(obj["code"])
            #如果数据获取失败
            if coin_hold_info == '' or coin_desc_info == '':
                fail_info.update({'code': obj["code"], 'name': obj["name"]})
                fail_collect.insert(fail_info)
            else:#获取成功
                coin_info.update(coin_hold_info)
                coin_info.update(coin_desc_info)
                print(coin_info)
                success_collect.insert(coin_info)

    except Exception as e:
        print(u'run_info执行货币'+obj["code"]+'信息抓取数据失败', e)

#按网页list页码获取币种CODE
def run_page(i):
    try:
        coin_list = get_coin_list(i)
        for strs in coin_list["data"]:
            obj=code_collect.find_one({'code':  strs["code"]})
            if obj is None:#如果数据库中没有，则插入
                code_collect.insert({'code': strs["code"], 'name': strs["name"], 'page': i})
    except Exception as e:
        print(u'获取货币详细数据失败', e)

def main():
    executor = ThreadPoolExecutor(max_workers=10)
    all_task = [executor.submit(run_page, (page)) for page in range(1,25)]

    all_task = [executor.submit(run_info, (page)) for page in range(1, 25)]
if __name__ == '__main__':
    s = mydb.test
    main()
#------------------------------------------------------------------------------attach----------
#https://dncapi.bqiapp.com/api/coin/holders?code=ethereum&side=30&webp=1 持币列表
#https://dncapi.bqiapp.com/api/coin/web-coinrank?page=2&type=0&pagesize=100&webp=1 货币列表
#简介信息https://www.feixiaohao.com/currencies/ethereum/
#url='https://dncapi.bqiapp.com/api/coin/holders?code=ethereum&side=30&webp=1'
#https://dncapi.bqiapp.com/api/coin/market_ticker?page=1&pagesize=100&code=bitcoin&token=&webp=1  交易详细数据

