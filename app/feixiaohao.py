#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import time

try:
    from app.package.network.dbopera import MongoDBOpera
    from app.package.network.webopera import WebOpera
    from app.package.format.webformat import WebFormat
    from app.package.data.logger import Logger
    from app.package.data.logger import log
    from concurrent.futures import ThreadPoolExecutor, as_completed
except Exception as e:
    from .package.network import MongoDBOpera
    from .package.network import WebOpera
    from .package.format import WebFormat
    from .package.data.logger import Logger


code_collect,desc_collect,holds_collect= None,None,None
holds_list,desc_list,holds_fail=[],[],[]

#logging.basicConfig(filename='dataget.log',filemode='a',format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                  #  level=logging.DEBUG)

def initDB():
    mo = MongoDBOpera()
    mo.connect('mongdb_cfg')
    global code_collect,desc_collect,holds_collect
    code_collect= mo.get_collection('local', 'code_Collection');
    desc_collect = mo.get_collection('local', 'desc_Collection');
    holds_collect = mo.get_collection('local', 'holds_Collection');

# 获取币种描述
def run_desc(code):
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
        localtime = time.strftime("%Y-%m-%d", time.localtime())
        desc_item={'CODE': code, "desc": desc_info[0].text, "pub_time": fxsj_info, "init_price": fxjg_info, "max": zdgy_info,
            "total": zgy_info, "markets": jys_info, "math": sf_info, "power": po_info,"get_time":localtime}
        MongoDBOpera().insert_to_collection(desc_collect,desc_item)
        log.logger.info(f"get_desc_info:get coin {code}description finished")
    except Exception as e:
        log.logger.error(f"get_desc_info:get coin {code}description failed", e)
        #log.logger.error(u"get_desc_info获取货币"+{code}+"描述数据失败", e)
    return code


# 按网页list页码获取币种CODE
def run_page(page):
    try:
        page_url = "https://dncapi.bqiapp.com/api/coin/web-coinrank?page=" + str(page) + "&type=0&pagesize=100&webp=1"
        wo = WebOpera(page_url)
        coin_list = wo.get_web_response()
        log.logger.info(f"begin get page:{page} coin list")
        #log.logger.info(u"开始获取第{"+page+"}页货币列表")
        if coin_list != '':
            for strs in coin_list["data"]:
                obj=MongoDBOpera().collection_find_one(code_collect,{'CODE': strs["code"]})
                if obj is None:  # 如果数据库中没有，则插入
                    MongoDBOpera().insert_to_collection(code_collect,{'CODE': strs["code"], 'name': strs["name"], 'page': page})
                    log.logger.info(f"find new coin:{strs['code']},insert CODE collection")
        else:
            log.logger.warning(f"page:{page} coin list get failed")
    except Exception as e:
        log.logger.error(f'run_page:get coin list page:{page} failed', e)
    return page

#获取持币信息
def run_holds(code):
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
            holds_store_info["get_time"] = time.strftime("%Y-%m-%d", time.localtime())
            MongoDBOpera().insert_to_collection(holds_collect, holds_store_info)
            log.logger.info(f"coin:{code} holds get finished")
        else:
            log.logger.warning(f"coin:{code} holds info response empty")
    except Exception as e:
        log.logger.error(f"run_holds:get coin:{code} holds info failed", e)
    return code


def get_list_diff(collectionA,collectionB,element="CODE",queryA=None,queryB=None):
    list_A,list_B,list_Diff=[],[],[]

    if(MongoDBOpera().is_collection(collectionA)):
        for item in MongoDBOpera().collection_find(collectionA,queryA):
            list_A.append(item[element])
    elif(isinstance(collectionA,list)):
        list_A=collectionA

    if (MongoDBOpera().is_collection(collectionB)):
        for item in MongoDBOpera().collection_find(collectionB,queryB):
            list_B.append(item[element])
    elif(isinstance(collectionB,list)):
        list_B=collectionB

    if list_B == []:
        list_Diff=list_A
    elif list_A == []:
        list_Diff=[]
    else:
        list_Diff = set(list_A).difference(set(list_B))

    return list_Diff


#执行循环等待
def run_wait(timesec):
    for i in range(timesec):
        time.sleep(1)
        deadline=int(timesec - i)
        hour=deadline//3600
        minus=(deadline%3600)//60
        sec=deadline%60
        log.logger.info(f"next run time count down:{hour}时:{minus}分:{sec}秒")



def main():
    while(1):
        try:
            localtime = time.strftime("%Y-%m-%d", time.localtime())

            log.logger.info(u'begin get coin basic info')

            executor = ThreadPoolExecutor(max_workers=24)
            all_task = [executor.submit(run_page, (page)) for page in range(1,25)]
            for future in as_completed(all_task,300):
                data = future.result()
            log.logger.info("coin basic info get finished")

            log.logger.info("begin get coin description")
            desc_diff_list=get_list_diff(code_collect,desc_collect,"CODE",None,{"get_time": localtime})
            if len(desc_diff_list) != 0:
                if len(desc_diff_list) > 50:
                    executor_holds = ThreadPoolExecutor(max_workers=50)
                else:
                    executor_holds = ThreadPoolExecutor(max_workers=len(desc_diff_list))

                all_task_holds = [executor_holds.submit(run_desc, item) for item in desc_diff_list]
                for future in as_completed(all_task_holds,1200):
                    code = future.result()
                    #log.logger.info(f"coin:{format(code)} description get finished.")
                #MongoDBOpera().insert_list_to_collection(desc_collect, desc_list)
            log.logger.info("all coins description finished")

            log.logger.info("begin get coin holds info")

            hold_diff_list = get_list_diff(code_collect, holds_collect, "CODE", None, {"get_time": localtime})
            if len(hold_diff_list) != 0:
                if len(hold_diff_list) > 50:
                    executor_holds = ThreadPoolExecutor(max_workers=50)
                else:
                    executor_holds = ThreadPoolExecutor(max_workers=len(hold_diff_list))
                all_task_holds = [executor_holds.submit(run_holds, item) for item in hold_diff_list]
                for future in as_completed(all_task_holds, 1200):
                    code = future.result()
                    #log.logger.info(f"coin:{format(code)} holds info get finished.")
                #MongoDBOpera().insert_list_to_collection(holds_collect, desc_list)
            log.logger.info("all coins holds info insert finished")
            run_wait(3600)
        except Exception as e:
            log.logger.error(u'main has some thread failed', e)
            run_wait(3600)



if __name__ == '__main__':

    initDB()
    #print(MongoDBOpera().is_collection(code_collect))
    main()

