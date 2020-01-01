import pandas as pd
from datetime import *
from sqlalchemy import *
import os
import http
import logging
from py_ctp.eventEngine import  *
from py_ctp.eventType import  *
import pymongo
import logging
import numpy as np
from py_ctp.ctp_struct import *
from PyQt5.QtWidgets import QApplication
import sys
import time as ttt
import threading

# region 基本的处理方法
def getWebServerTime(web):  # 调整本地时间，从 百度 上获取北京时间，然后增加 1.5 秒后进行本地时间的更改
    conn = http.client.HTTPConnection(web)
    conn.request("GET", "/")
    r = conn.getresponse()
    ts = r.getheader('date')  # 获取http头date部分
    # 将GMT时间转换成北京时间
    ltime = datetime.strptime(ts[5:25], "%d %b %Y %H:%M:%S") + timedelta(hours=8, seconds=1.5)
    dat = "date %u-%02u-%02u" % (ltime.year, ltime.month, ltime.day)
    tm = "time %02u:%02u:%02u" % (ltime.hour, ltime.minute, ltime.second)
    os.system(dat)
    os.system(tm)

def getLoseData(goodsCode, freq, startTime, endTime):  # 得到理论上，我们在这个时间范围上应该获取的数据， 包括 startTime， 不包括 endTime
    seriesTradeDay = tradeDatetime.copy()
    theStartTime = startTime.strftime('%Y-%m-%d')
    theEndtime = endTime.strftime('%Y-%m-%d')
    if startTime.isoweekday() == 6:  # 如果为
        listTemp = [seriesTradeDay[(seriesTradeDay <= pd.to_datetime(theStartTime))].iat[-1]]
        listTemp.extend(seriesTradeDay[(seriesTradeDay >= pd.to_datetime(theStartTime))
                                        & (seriesTradeDay <= pd.to_datetime(theEndtime))].tolist())
        seriesTradeDay = listTemp
    else:
        seriesTradeDay = seriesTradeDay[(seriesTradeDay >= pd.to_datetime(theStartTime))
                                        & (seriesTradeDay <= pd.to_datetime(theEndtime))].tolist()
    listTradeTime = []
    for eachDay in seriesTradeDay:
        if eachDay.date() not in listHolidayDate:
            listTradeTime.extend(list(map(lambda x:eachDay + timedelta(days = 1 if x.hour in [0, 1, 2] else 0, hours=x.hour, minutes=x.minute), dictFreqGoodsCloseNight[freq][goodsCode])))
        else:
            listTradeTime.extend(list(map(lambda x:eachDay + timedelta(days = 1 if x.hour in [0, 1, 2] else 0, hours=x.hour, minutes=x.minute),
                                          dictFreqGoodsCloseNight[freq][goodsCode][:dictFreqGoodsCloseNight[freq][goodsCode].index(dictFreqGoodsClose[1][goodsCode][-1]) + 1])))
    seriesTradeTime = pd.Series(listTradeTime).sort_values()
    seriesTradeTime = seriesTradeTime[(seriesTradeTime > startTime) & (seriesTradeTime <= endTime)].reset_index(drop = True)
    return seriesTradeTime

def downLogProgram(log):
    print(str(log))
    logProgram.info(str(log))

def downLogBarDeal(log, freq):
    dictFreqLog[freq].info(str(log))

def downLogTick(log):
    logTick.info(str(log))

def readMongoNum(db, name, num):  # 读取 mongodb 的数据库， 读取最近的 n
    cursor = db[name].find(limit = num, sort = [("trade_time", pymongo.DESCENDING)])  # 读取 mongodb， 因为一个软件只使用一个数据库吧
    df = pd.DataFrame(list(cursor))
    df.drop(['_id'], axis=1, inplace = True)
    return df

def dfInsertMongo(df, con, index = True):
    if index:
        df = df.reset_index(drop = False)
    listTemp = []
    for i in range(df.shape[0]):
        dictTemp = df.iloc[i].to_dict()
        dictTemp = insertDbChg(dictTemp)
        listTemp.append(dictTemp)
    con.insert_many(listTemp)

def insertDbChg(dict):  # 主要用于更改数据类型
    for each in dict.keys():
        if isinstance(dict[each], np.int64):
            dict[each] = int(dict[each])
        elif isinstance(dict[each], np.float64):
            dict[each] = float(dict[each])
        elif isinstance(dict[each], np.int32):
            dict[each] = int(dict[each])
        if isinstance(dict[each], float):
            dict[each] = round(dict[each], 4)
        # elif isinstance(dict[each], pd._libs.tslib.Timestamp):
        #     dict[each] = dict[each].strftime("%Y-%m-%d %H:%M:%S")
    return dict

def readMongoGTTime(db, name, time):  # 读取 mongodb 的数据库， 这个为 db.tradeTime > time
    cursor = db[name].find({"trade_time": { "$gt": time }}, limit = num, sort = [("trade_time", pymongo.ASCENDING)])  # 读取 mongodb， 因为一个软件只使用一个数据库吧
    df = pd.DataFrame(list(cursor))
    df.drop(['_id'], axis=1, inplace = True)
    return df

def readMongoGTETime(db, name, time):  # 读取 mongodb 的数据库， 这个为 db.tradeTime >= time
    cursor = db[name].find({"trade_time": { "$gte": time }}, limit = num, sort = [("trade_time", pymongo.ASCENDING)])  # 读取 mongodb， 因为一个软件只使用一个数据库吧
    df = pd.DataFrame(list(cursor))
    df.drop(['_id'], axis=1, inplace = True)
    return df
# endregion

getWebServerTime('www.baidu.com')  # 更改本地时间

listFreq = list(range(5, 25))
listFreqPlus = listFreq.copy()
listFreqPlus.insert(0, 1)  # 基础频率加上一分钟频率
mvLenVector = [80, 100, 120, 140, 160, 180, 200, 220, 240, 260]  # 均值的长度
# 建立主引擎
ee = EventEngine()
# socket 的 port
port = 8888
# 数据库地址
databaseIP = 'localhost'

# region 读取 Information 信息
dictLoginInformation = {}
with open('RD files\\LoginInformation.txt', 'r', encoding='UTF-8') as f:
    for row in f:
        if 'userid' in row:
            dictLoginInformation['userid'] = row.split('：')[1].strip()
        if 'password' in row:
            dictLoginInformation['password'] = row.split('：')[1].strip()
        if 'broker' in row:
            dictLoginInformation['broker'] = row.split('：')[1].strip()
        if 'front_addr' in row:
            dictLoginInformation['front_addr'] = row.split('：')[1].strip()
        if 'product_info' in row:
            dictLoginInformation['product_info'] = row.split('：')[1].strip()
        if 'app_id' in row:
            dictLoginInformation['app_id'] = row.split('：')[1].strip()
        if 'auth_code' in row:
            dictLoginInformation['auth_code'] = row.split('：')[1].strip()
# endregion

# region 获取交易日
dfDatetime = pd.read_excel('RD files\\公共参数.xlsx', sheet_name='期货交易日')  # 期货交易日的表格
tradeDatetime = dfDatetime['tradeDatetime']
listHolidayDate = tradeDatetime[dfDatetime['holiday'] == 1].dt.date.tolist()  # 节假日的时间
now = datetime.now()
# endregion

# region 获取品种信息
GoodsTab = pd.read_excel('RD files\\公共参数.xlsx', sheet_name='品种信息', index_col='品种名称')  # 读取 公共参数
dictCloseTimeClose = pd.read_pickle('pickle\\dictCloseTimeClose.pkl')  # 时间区间
dictCloseTimeCloseNight = pd.read_pickle('pickle\\dictCloseTimeCloseNight.pkl')  # 时间区间
dictGoodsName = {}  # 品种名称对应品种编号
listGoods = []  # 品种编号
dictGoodsChg = {}  # 品种名称对应交易所名称
dictFreqGoodsClose = {}  # 夜盘开始，15：00结束
dictFreqGoodsCloseNight = {}  # 日盘开始，夜盘交易时间结束
dictGoodsSend = {}  # 发送记录时间
dictGoodsFront = {}  # 品种的 前时间
dictGoodsLast = {}  # 夜盘收盘时间
dictGoodsLastWord = {}  # 夜盘收盘时间中文显示
dictGoodsOneMin = {}  # 能够记录分钟数据的合成情况，只记录一分钟数据
dictGoodsVolume = {}  # 品种对应的 总成交量 与 总成交额
for num in range(GoodsTab.shape[0]):
    goodsCode = GoodsTab['品种代码'][num]
    dictGoodsName[goodsCode] = GoodsTab.index[num]
    dictGoodsChg[goodsCode.split('.')[0]] = goodsCode.split('.')[1]
    dictGoodsLastWord[goodsCode] = GoodsTab['交易时间类型'][num]
    listGoods.append(goodsCode)
    if goodsCode.split('.')[1] != "CFE":
        dictGoodsSend[goodsCode] = [time(10, 15), time(11, 30), time(15)]
        dictGoodsFront[goodsCode] = [time(9, 1), time(10, 31), time(13, 31)]
    else:
        dictGoodsSend[goodsCode] = [time(11, 30), time(15, 15)]
        dictGoodsFront[goodsCode] = [time(9, 16), time(13, 1)]
    if dictGoodsLastWord[goodsCode] == '23.00收盘':
        dictGoodsSend[goodsCode].append(time(23))
        dictGoodsFront[goodsCode].append(time(21, 1))
    elif dictGoodsLastWord[goodsCode] == '23.30收盘':
        dictGoodsSend[goodsCode].append(time(23, 30))
        dictGoodsFront[goodsCode].append(time(21, 1))
    elif dictGoodsLastWord[goodsCode] == '1.00收盘':
        dictGoodsSend[goodsCode].append(time(1))
        dictGoodsFront[goodsCode].append(time(21, 1))
    elif dictGoodsLastWord[goodsCode] == '2.30收盘':
        dictGoodsSend[goodsCode].append(time(2, 30))
        dictGoodsFront[goodsCode].append(time(21, 1))
for freq in listFreqPlus:
    dictFreqGoodsClose[freq] = {}
    dictFreqGoodsCloseNight[freq] = {}
    for goodsCode in listGoods:
        if dictGoodsLastWord[goodsCode] == '15.00收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(15)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(15)]
        elif dictGoodsLastWord[goodsCode] == '15.15收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(15, 15)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(15, 15)]
        elif dictGoodsLastWord[goodsCode] == '23.00收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(23)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(23)]
        elif dictGoodsLastWord[goodsCode] == '23.30收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(23, 30)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(23, 30)]
        elif dictGoodsLastWord[goodsCode] == '1.00收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(1)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(1)]
        elif dictGoodsLastWord[goodsCode] == '2.30收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(2, 30)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(2, 30)]
        if freq == 1:
            dictGoodsLast[goodsCode] = dictFreqGoodsCloseNight[1][goodsCode][-1]
            dictGoodsOneMin[goodsCode] = dictFreqGoodsClose[1][goodsCode]
# endregion

# region 列名的处理
listTick = ['tradeTime', 'goodsCode', 'close', 'volume', 'amt', 'position']
listMin = ['goods_code', 'goods_name', 'open', 'high', 'low', 'close', 'volume', 'amt', 'oi']
listAdjust = ['goods_code', 'goods_name', 'adjdate', 'adjinterval']
listMa = ['goods_code', 'goods_name', 'open', 'high', 'low', 'close']
for vector in mvLenVector:
    listMa.extend(['maprice_{}'.format(vector), 'stdprice_{}'.format(vector), 'stdmux_{}'.format(vector), 'highstdmux_{}'.format(vector), 'lowstdmux_{}'.format(vector)])
listOverLap = ['goods_code', 'goods_name', 'open', 'high', 'low', 'close']
for vector in mvLenVector:
    listOverLap.extend(['重叠度高_{}'.format(vector), '重叠度低_{}'.format(vector), '重叠度收_{}'.format(vector)])
# endregion

# 合约切换表
dictGoodsAdj = {}  # 合约切换表的记录
dictGoodsInstrument = {}  # 品种名称与主力合约的映射关系
listInstrument = []

# 建立 mongodb 的连接
dictFreqCon = {}
dictData = {}
dictGoodsTick = {}  # 储存 Tick 数据
readNum = 300
myclient = pymongo.MongoClient("mongodb://{}:27017/".format(databaseIP))
for freq in listFreqPlus:
    # 创建数据库，与建立数据库连接
    dictFreqCon[freq] = myclient["cta{}_trade".format(freq)]

# 建立日志
theTradeDay = tradeDatetime[tradeDatetime >= now - timedelta(hours=16)].iat[0]  # 确定交易日
loggingPath = 'log\\{}'.format(theTradeDay.strftime('%Y-%m-%d'))
os.makedirs(loggingPath, exist_ok=True)
logProgram = logging.getLogger('logProgram')
fileHandle = logging.FileHandler(loggingPath + '\\logProgram.txt')
fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logProgram.addHandler(fileHandle)
logProgram.setLevel(logging.INFO)
# 接收Tick数据的记录
logTick = logging.getLogger('logTick')
fileHandle = logging.FileHandler(loggingPath + '\\logTick.txt')
fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logTick.addHandler(fileHandle)
logTick.setLevel(logging.INFO)
# 接收Bar到处理日记文件
dictFreqLog = {}
for freq in listFreq:
    theLog = logging.getLogger('CTA{}'.format(freq))
    fileHandle = logging.FileHandler(loggingPath + '\\CTA{}.txt'.format(freq))
    fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    theLog.addHandler(fileHandle)
    theLog.setLevel(logging.INFO)
    dictFreqLog[freq] = theLog  # 对应的分钟处理方法

