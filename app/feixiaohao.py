#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
import sys
import time

#sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from app.package.network.dbopera import MongoDBOpera
    from app.package.network.webopera import WebOpera
    from app.package.format.webformat import WebFormat
except Exception as e:
    from package.network import MongoDBOpera
    from package.network import WebOpera
    from package.format import WebFormat

from concurrent.futures import ThreadPoolExecutor,as_completed



code_collect = None
desc_collect = None
holds_collect = None
holds_list=[]
market_list=[]

logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)

def initDB():
    mo = MongoDBOpera()
    mo.connect('mongdb_cfg')
    global code_collect,desc_collect,holds_collect
    code_collect= mo.get_collection('local', 'code_Collection');
    desc_collect = mo.get_collection('local', 'desc_Collection');
    holds_collect = mo.get_collection('local', 'holds_Collection');

# 获取币种描述
def get_desc_info(code):
    desc_url = "https://www.feixiaohao.com/currencies/" + str(code)
    try:
        wo = WebOpera(desc_url)
        desc_data = (wo.get_web_html())
        infos = WebFormat().check_html(desc_data)
        desc_info = infos.xpath("//div[@class='coinIntroduce']/div[@class='textBox']")
        fxsj_info = infos.xpath(
            "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[
            0]
        fxjg_info = infos.xpath(
            "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[
            1]
        zdgy_info = infos.xpath(
            "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[
            2]
        zgy_info = infos.xpath(
            "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[
            3]
        jys_info = infos.xpath(
            "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[
            4]
        sf_info = infos.xpath(
            "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[
            5]
        po_info = infos.xpath(
            "//div[@class='coinIntroduce']/div[@class='infoList']/div[@class='listRow']/div[@class='listCell']/span[@class='val']/text()")[
            6]
    except Exception as e:
        logging.error(u'get_desc_info获取货币' + code + '描述数据失败', e)
        return ''
    return {'CODE': code, "描述": desc_info[0].text, "发行时间": fxsj_info, "发行价格": fxjg_info, "最大供应": zdgy_info,
            "总供应": zgy_info, "上架交易所": jys_info, "算法": sf_info, "激励机制": po_info}


# 获取币种列表
def get_coin_list(page):
    page_url = "https://dncapi.bqiapp.com/api/coin/web-coinrank?page=" + str(page) + "&type=0&pagesize=100&webp=1"
    try:
        wo = WebOpera(page_url)
        coin_list = wo.get_web_response()
        logging.info('开始获取第'+str(page)+"页货币列表")
    except Exception as e:
        logging.error(u'get_coin_list获取货币列表页' + str(page) + '数据失败', e)
        return ''
    return coin_list

# 按网页list页码获取币种CODE
def run_page(page):
    try:
        coin_list = get_coin_list(page)
        if coin_list != '':
            for strs in coin_list["data"]:
                obj=MongoDBOpera().collection_find_one(code_collect,{'code': strs["code"]})
                if obj is None:  # 如果数据库中没有，则插入
                    MongoDBOpera().insert_to_collection(code_collect,{'code': strs["code"], 'name': strs["name"], 'page': page})
                    logging.info('发现新币种:' + strs["code"] + ",插入CODE集合")
                    desc=get_desc_info(strs["code"])
                    if desc != '':
                        MongoDBOpera().insert_to_collection(desc_collect,desc)
                        logging.info('获取币种:' + strs["code"] + "描述信息,插入DESC集合")
                    else:
                        logging.info('获取币种:' + strs["code"] + "描述信息失败，未插入DESC集合")
        else:
            logging.warning("第"+str(page)+"页货币列表获取失败")
        return page
    except Exception as e:
        logging.error(u'run_page获取货币列表页' + str(page) + '数据失败', e)


#获取持币信息
def run_holds(item):
    code = item['code']
    holds_url = "https://dncapi.bqiapp.com/api/coin/holders?code=" + str(code) + "&side=30&webp=1"
    try:
        wo = WebOpera(holds_url)
        response_data = wo.get_web_response()
        if response_data != '':
            hold_info = response_data["data"]
            holds_store_info = {'CODE':code,'total_holders': hold_info["total_holders"],
                                'percentage_top10': hold_info["percentage_top10"],
                                'percentage_top20': hold_info["percentage_top20"],
                                'percentage_top50': hold_info["percentage_top50"],
                                'update_time': hold_info["update_time"],
                                'holders_list': hold_info["holders_list"]}
            holds_store_info["local_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            holds_list.append(holds_store_info)
        return code
    except Exception as e:
        logging.error(u'run_holds获取货币' + code + '持币数据失败', e)

#获取交易数据信息
def run_market(item):
    try:
        code=item["code"]
        for i in range(1,7):
            market_url="https://dncapi.bqiapp.com/api/coin/market_ticker?page="+str(i)+"&pagesize=100&code="+code+"&token=&webp=1"
            wo=WebOpera(market_url)
            response_data = wo.get_web_response()
            if response_data != '':
                global market_list
                market=response_data["data"]
                for info in market:
                    info["local_time"]=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    market_list.append(info)
        return code
    except Exception as e:
        logging.error(u'run_market获取货币' + code + '市场数据失败', e)



def multi_thread(task):
    if task == 'CODE':
        executor = ThreadPoolExecutor(max_workers=10)
        all_task = [executor.submit(run_page, (page)) for page in range(1, 25)]
    elif task == 'HOLDS':
        executor_holds = ThreadPoolExecutor(max_workers=10)
        all_task_holds = [executor_holds.submit(run_holds, (code)) for code in code_collect.find()]
    elif task == 'MARKET':
        executor_market = ThreadPoolExecutor(max_workers=10)
        all_task_holds = [executor_market.submit(run_market, (code)) for code in code_collect.find()]

def run_wait():
    for i in range(60 * 60 * 24):
        time.sleep(1)
        deadline=int(60 * 60 * 24 - i)
        hour=deadline//3600
        minus=(deadline%3600)//60
        sec=deadline%60
        logging.info(f"下次获取货币信息倒计时:{hour}时:{minus}分:{sec}秒")
def main():
    #logging.info("=====================" + os.getcwd())
    while(1):

        logging.info("开始获取币种基础信息")
        executor = ThreadPoolExecutor(max_workers=24)
        all_task = [executor.submit(run_page, (page)) for page in range(1,25)]
        for future in as_completed(all_task,300):
            data = future.result()
        logging.info("币种基础信息获取完成")


       # '''
        try:
            logging.info("开始获取币种持币信息")
            executor_holds = ThreadPoolExecutor(max_workers=50)
            all_task_holds = [executor_holds.submit(run_holds, (code)) for code in code_collect.find()]
            for future in as_completed(all_task_holds,300):
                code = future.result()
                logging.info("币种:{} 对应持币信息获取完成.".format(code))
            MongoDBOpera().insert_list_to_collection(holds_collect,holds_list)
            logging.info("币种持币信息插入集合完成")
            holds_list.clear()
            run_wait()
        except Exception as e:
            logging.error(u'main存在线程执行失败', e)
    '''
        executor_market = ThreadPoolExecutor(max_workers=200)
        all_task_markert = [executor_market.submit(run_market, (code)) for code in code_collect.find()]
        for future in as_completed(all_task_markert):
            code = future.result()
            print("in main: get market {} success".format(code))
        print("任务全部完成")
        MongoDBOpera().insert_list_to_collection(holds_collect, market_list)
    '''


if __name__ == '__main__':
    initDB()
    main()

