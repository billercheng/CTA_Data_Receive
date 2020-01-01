from parameter import *
from getMa import *
from getOverLapDegree import *
from chgAdjust import *

def completeDb():
    # 获取合约切换表
    checkChg()
    # 计算多分钟数据的完整性
    for freq in listFreq:
        checkOtherMinBar(freq)  # 检查其它频段数据与调整表
        getMa(freq)  # 生成均值表
        getOverLapDegree(freq)  # 生成重叠度表

def checkOtherMinBar(freq):  # 合成其它分钟的程序
    con = dictFreqCon[freq]
    downLogProgram("检查 CTA{} 数据完整性".format(freq))
    for goodsCode in dictGoodsName.keys():
        # 读取分钟数据
        goodsName = dictGoodsName[goodsCode]
        dfMinute = dictData[freq][goodsName + '_调整表'][-1:].copy()
        if dfMinute.shape[0] != 0:
            startTime = dfMinute.index[0]
        else:
            startTime = dictData[1][goodsName + '_调整表'].index[0]
        dfFreqAll = dictData[1][goodsName + '_调整表'].copy()
        dfFreq = dfFreqAll[dfFreqAll.index > startTime]
        if dfFreq.shape[0] > 0:
            listMinute = getLoseData(goodsCode, freq, startTime, dfFreq.index[-1])
            if True:
                dfFreq['trade_time'] = np.nan
                # 开始合成
                for i in range(len(dictFreqGoodsClose[freq][goodsCode])):
                    if i != len(dictFreqGoodsClose[freq][goodsCode]) - 1:
                        listTemp = (dfFreq.index.time == dictFreqGoodsClose[freq][goodsCode][i + 1]).tolist()
                        dfFreq.loc[listTemp, 'trade_time'] = dfFreq.loc[listTemp].index
                    else:
                        listTemp = (dfFreq.index.time == dictFreqGoodsClose[freq][goodsCode][0]).tolist()
                        dfFreq.loc[listTemp, 'trade_time'] = dfFreq.loc[listTemp].index
                dfFreq['trade_time'] = dfFreq['trade_time'].bfill()
                dfFreq = dfFreq[~dfFreq['trade_time'].isnull()]
                dfFreq.index.name = 'trade_time_index'
                dfFreq = dfFreq.groupby(by='trade_time').agg({'goods_code': 'last',
                                                              'goods_name': 'last',
                                                              'close': 'last',
                                                              'open': 'first',
                                                              'high': max,
                                                              'low': min,
                                                              'volume': sum,
                                                              'amt': sum,
                                                              'oi': 'last'})
                dfFreq = dfFreq[listMin]
                # 写入数据操作
                if dfFreq.shape[0] > 0:
                    table = con[dictGoodsName[goodsCode] + '_调整表']
                    dfInsertMongo(dfFreq, table, index=True)
                dictData[freq][dictGoodsName[goodsCode] + '_调整表'] = dictData[freq][dictGoodsName[goodsCode] + '_调整表'].append(dfFreq)


