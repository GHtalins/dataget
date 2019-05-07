#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import time

try:
    from app.package.network.dbopera import MongoDBOpera
    from app.package.network.webopera import WebOpera
    from app.package.format.webformat import WebFormat
    from concurrent.futures import ThreadPoolExecutor, as_completed
except Exception as e:
    from .package.network import MongoDBOpera
    from .package.network import WebOpera
    from .package.format import WebFormat



code_collect,desc_collect,holds_collect= None,None,None
holds_list,desc_list,holds_fail=[],[],[]
FAIL_FLAG=False
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

    except Exception as e:
        logging.error(f"get_desc_info获取货币{code}描述数据失败", e)
    return code


# 按网页list页码获取币种CODE
def run_page(page):
    try:
        page_url = "https://dncapi.bqiapp.com/api/coin/web-coinrank?page=" + str(page) + "&type=0&pagesize=100&webp=1"
        wo = WebOpera(page_url)
        coin_list = wo.get_web_response()
        logging.info(f"开始获取第{page}页货币列表")
        if coin_list != '':
            for strs in coin_list["data"]:
                obj=MongoDBOpera().collection_find_one(code_collect,{'CODE': strs["code"]})
                if obj is None:  # 如果数据库中没有，则插入
                    MongoDBOpera().insert_to_collection(code_collect,{'CODE': strs["code"], 'name': strs["name"], 'page': page})
                    logging.info(f"发现新币种:{strs['code']},插入CODE集合")
        else:
            logging.warning(f"第{page}页货币列表获取失败")
    except Exception as e:
        logging.error(f'run_page获取货币列表页{page}数据失败', e)
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
        else:
            logging.warning(f"币种{code}持币信息返回为空，获取失败")
    except Exception as e:
        logging.error(f"run_holds获取货币{code}持币数据失败", e)
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
        logging.info(f"下次获取货币信息倒计时:{hour}时:{minus}分:{sec}秒")



def main():
    while(1):
        try:
            localtime = time.strftime("%Y-%m-%d", time.localtime())

            logging.info("开始获取币种基础信息")
            executor = ThreadPoolExecutor(max_workers=24)
            all_task = [executor.submit(run_page, (page)) for page in range(1,25)]
            for future in as_completed(all_task,300):
                data = future.result()
            logging.info("币种基础信息获取完成")



            logging.info("开始获取币种描述信息")
            desc_diff_list=get_list_diff(code_collect,desc_collect,"CODE",None,{"get_time": localtime})
            if len(desc_diff_list) != 0:
                if len(desc_diff_list) > 50:
                    executor_holds = ThreadPoolExecutor(max_workers=50)
                else:
                    executor_holds = ThreadPoolExecutor(max_workers=len(desc_diff_list))

                all_task_holds = [executor_holds.submit(run_desc, item) for item in desc_diff_list]
                for future in as_completed(all_task_holds,600):
                    code = future.result()
                    logging.info(f"币种:{format(code)} 对应描述信息获取完成.")
                #MongoDBOpera().insert_list_to_collection(desc_collect, desc_list)
            logging.info("币种描述信息获取完成")


            logging.info("开始获取币种持币信息")

            hold_diff_list = get_list_diff(code_collect, holds_collect, "CODE", None, {"get_time": localtime})
            if len(hold_diff_list) != 0:
                if len(hold_diff_list) > 50:
                    executor_holds = ThreadPoolExecutor(max_workers=50)
                else:
                    executor_holds = ThreadPoolExecutor(max_workers=len(hold_diff_list))
                all_task_holds = [executor_holds.submit(run_holds, item) for item in hold_diff_list]
                for future in as_completed(all_task_holds, 600):
                    code = future.result()
                    logging.info(f"币种:{format(code)} 对应持币信息获取完成.")
                #MongoDBOpera().insert_list_to_collection(holds_collect, desc_list)
            logging.info("币种持币信息插入集合完成")
            run_wait(7200)
        except Exception as e:
            logging.error(u'main存在线程执行失败,本次不插入新补充的币种描述和持币信息', e)
            run_wait(7200)



if __name__ == '__main__':

    initDB()
    #print(MongoDBOpera().is_collection(code_collect))
    main()

