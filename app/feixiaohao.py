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
holds_list,market_list,holds_fail=[],[],[]
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
        localtime = time.strftime("%Y-%m-%d", time.localtime())
    except Exception as e:
        logging.error(f"get_desc_info获取货币{code}描述数据失败", e)
        return ''
    return {'CODE': code, "desc": desc_info[0].text, "pub_time": fxsj_info, "init_price": fxjg_info, "max": zdgy_info,
            "total": zgy_info, "markets": jys_info, "math": sf_info, "power": po_info,"get_time":localtime}


# 获取币种列表
def get_coin_list(page):
    page_url = "https://dncapi.bqiapp.com/api/coin/web-coinrank?page=" + str(page) + "&type=0&pagesize=100&webp=1"
    try:
        wo = WebOpera(page_url)
        coin_list = wo.get_web_response()
        logging.info(f"开始获取第{page}页货币列表")
    except Exception as e:
        logging.error(f"get_coin_list获取货币列表页{page}数据失败", e)
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
                    logging.info(f"发现新币种:{strs['code']},插入CODE集合")
                    desc=get_desc_info(strs["code"])
                    if desc != '':
                        MongoDBOpera().insert_to_collection(desc_collect,desc)
                        logging.info(f"获取币种{strs['code']}描述信息，插入DESC集合")
                    else:
                        logging.info(f"获取币种{strs['code']}描述信息失败，未插入DESC集合")
        else:
            logging.warning(f"第{page}页货币列表获取失败")

        return page
    except Exception as e:
        logging.error(f'run_page获取货币列表页{page}数据失败', e)

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
            holds_list.append(holds_store_info)
        else:
            logging.warning(f"币种{code}持币信息返回为空，获取失败")
        return code
    except Exception as e:
        logging.error(f"run_holds获取货币{code}持币数据失败", e)

def get_one_hold(code):
    holds_url = "https://dncapi.bqiapp.com/api/coin/holders?code=" + str(code) + "&side=30&webp=1"
    try:
        wo = WebOpera(holds_url)
        response_data = wo.get_web_response()
        if response_data != '':
            hold_info = response_data["data"]
            holds_store_info = {'CODE': code, 'total_holders': hold_info["total_holders"],
                                'percentage_top10': hold_info["percentage_top10"],
                                'percentage_top20': hold_info["percentage_top20"],
                                'percentage_top50': hold_info["percentage_top50"],
                                'update_time': hold_info["update_time"],
                                'holders_list': hold_info["holders_list"]}
            holds_store_info["get_time"] = time.strftime("%Y-%m-%d", time.localtime())
        else:
            logging.warning(f"币种{code}持币信息返回为空，获取失败")
            holds_store_info=''
        return holds_store_info
    except Exception as e:
        logging.error(f"run_holds获取货币{code}持币数据失败", e)
        return ''

#获取交易数据信息，未使用
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
        logging.error(f"run_market获取货币{code}市场数据失败", e)

#执行失败币种列表和检查币种描述信息缺失情况
def run_fail():
    logging.info("开始执行失败列表")
    cmp_code,cmp_holds,cmp_desc = [],[],[]
    localtime = time.strftime("%Y-%m-%d", time.localtime())
    try:
        for code_item in MongoDBOpera().collection_find(code_collect):
            cmp_code.append(code_item["code"])

        logging.info("开始执行币种持币信息失败列表")
        for hold_item in MongoDBOpera().collection_find(holds_collect,{"get_time": localtime}):
            cmp_holds.append(hold_item["CODE"])
        list_hold_cmp = set(cmp_code).difference(set(cmp_holds))
        for code in list_hold_cmp:
            hold_info = get_one_hold(code)
            if hold_info != '':
                MongoDBOpera().insert_to_collection(holds_collect, hold_info)
                logging.info(f"获取币种{code}持币信息,插入HOLD集合")
            else:
                logging.info(f"获取币种{code}持币信息失败，未插入HOLD集合")
        logging.info("缺失币种持币信息插入集合完成")

        logging.info("开始执行币种描述信息缺失补充")

        for desc_item in MongoDBOpera().collection_find(desc_collect,{"get_time": localtime}):
            cmp_desc.append(desc_item["CODE"])
        list_cmp=set(cmp_code).difference(set(cmp_desc))
        for desc in list_cmp:
            desc_info = get_desc_info(desc)
            if desc_info != '':
                MongoDBOpera().insert_to_collection(desc_collect, desc_info)
                logging.info(f"获取币种{desc}描述信息,插入DESC集合")
            else:
                logging.info(f"获取币种{desc}描述信息失败，未插入DESC集合")
        logging.info("失败列表执行完成")
    except Exception as e:
        logging.error("失败列表执行存在错误，但会忽略")


#执行循环等待
def run_wait():
    for i in range(60 * 60 * 24):
        time.sleep(1)
        deadline=int(60 * 60 * 24 - i)
        hour=deadline//3600
        minus=(deadline%3600)//60
        sec=deadline%60
        logging.info(f"下次获取货币信息倒计时:{hour}时:{minus}分:{sec}秒")



def main():
    while(1):
        try:
            holds_list.clear()
            FAIL_FLAG=True
            logging.info("开始获取币种基础信息")
            executor = ThreadPoolExecutor(max_workers=24)
            all_task = [executor.submit(run_page, (page)) for page in range(1,25)]
            for future in as_completed(all_task,90):
                data = future.result()
            logging.info("币种基础信息获取完成，开始获取币种持币信息")

            executor_holds = ThreadPoolExecutor(max_workers=50)
            all_task_holds = [executor_holds.submit(run_holds, (item["code"])) for item in code_collect.find()]
            for future in as_completed(all_task_holds,120):
                code = future.result()
                logging.info(f"币种:{format(code)} 对应持币信息获取完成.")
            MongoDBOpera().insert_list_to_collection(holds_collect,holds_list)
            logging.info("币种持币信息插入集合完成")
        except Exception as e:
            logging.error(u'main存在线程执行失败,但任然插入币种持币信息', e)
            MongoDBOpera().insert_list_to_collection(holds_collect, holds_list)
            logging.info("币种持币信息插入集合完成")
        run_fail()
        run_wait()



if __name__ == '__main__':
    initDB()
    main()

