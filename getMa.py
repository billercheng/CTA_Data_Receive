from chgAdjust import *
from parameter import *
from onBar import *

"""分钟数据过滤处理"""
def getMa(freq):
    con = dictFreqCon[freq]
    downLogProgram("检查 CTA{} 均值数据".format(freq))
    for goodsCode in dictGoodsName.keys():
        goodsName = dictGoodsName[goodsCode]
        if dictData[freq][dictGoodsName[goodsCode] + '_均值表'].shape[0] > 0:
            startTime = dictData[freq][dictGoodsName[goodsCode] + '_均值表'].index[-1]
        else:
            startTime = dictData[1][goodsName + '_调整表'].index[0]
        endTime = dictData[freq][dictGoodsName[goodsCode] + '_调整表'].index[-1]
        listCurrentTime = list(dictData[freq][dictGoodsName[goodsCode] + '_调整表'][dictData[freq][dictGoodsName[goodsCode] + '_调整表'].index > startTime].index)
        # if endTime - startTime > timedelta(hours=1):
        if True:
            dfFreqAll = dictData[freq][dictGoodsName[goodsCode] + '_调整表'].copy()
            dfFreqAll['amt'] = dfFreqAll['close'] * dfFreqAll['volume']
            dfFreqResult = dfFreqAll.copy()
            dfAdjustAll = dictGoodsAdj[goodsCode].copy()
            for mvl in mvLenVector:
                dfFreq = dfFreqAll[
                          max(dfFreqAll[dfFreqAll.index <= startTime].shape[0] - mvl + 1, 0):].copy()
                dfFreq['maprice_{}'.format(mvl)] = 0.0
                dfFreq['stdprice_{}'.format(mvl)] = 0.0
                dfFreq['stdmux_{}'.format(mvl)] = 0.0
                dfFreq['highstdmux_{}'.format(mvl)] = 0.0
                dfFreq['lowstdmux_{}'.format(mvl)] = 0.0
                dfFreqResult['maprice_{}'.format(mvl)] = 0.0
                dfFreqResult['stdprice_{}'.format(mvl)] = 0.0
                dfFreqResult['stdmux_{}'.format(mvl)] = 0.0
                dfFreqResult['highstdmux_{}'.format(mvl)] = 0.0
                dfFreqResult['lowstdmux_{}'.format(mvl)] = 0.0
                dfAdj = dfAdjustAll[
                    (dfAdjustAll['adjdate'] > dfFreq.index[0]) & (dfAdjustAll['adjdate'] < endTime)]
                if dfAdj.shape[0] > 0:
                    dfFreqResult.update(getMaStdGeneral(dfFreq[dfFreq.index < dfAdj['adjdate'][0]].copy(), mvl))
                    for eachNum in range(dfAdj.shape[0]):
                        loc = dfFreq[dfFreq.index < dfAdj['adjdate'][eachNum]].shape[0]  # 调整时刻位置
                        locLeft = max(loc - mvl + 1, 0)
                        dfFreq['close'][locLeft:loc] = dfFreq['close'][locLeft:loc] + \
                                                              dfAdj['adjinterval'][eachNum]
                        dfFreq['amt'][locLeft:loc] = dfFreq['close'][locLeft:loc] * dfFreq['volume'][
                                                                                                  locLeft:loc]
                        if eachNum != dfAdj.shape[0] - 1:
                            loc_before = dfFreq[dfFreq.index < dfAdj['adjdate'][eachNum + 1]].shape[0]  # 调整时刻位置
                            dfFreqResult.update(getMaStdGeneral(dfFreq[locLeft:loc_before].copy(), mvl))
                        else:
                            dfFreqResult.update(getMaStdGeneral(dfFreq[locLeft:].copy(), mvl))
                else:
                    dfFreqResult.update(getMaStdGeneral(dfFreq.copy(), mvl))
            dfFreqResult = dfFreqResult[dfFreqResult.index > startTime]
            dfFreqResult = dfFreqResult.drop(['volume', 'amt', 'oi'], axis=1)
            if dfFreqResult.shape[0] > 0:
                table = con[dictGoodsName[goodsCode] + '_均值表']
                dfInsertMongo(dfFreqResult, table, index=True)
            dictData[freq][dictGoodsName[goodsCode] + '_均值表'] = dictData[freq][dictGoodsName[goodsCode] + '_均值表'].append(dfFreqResult)
        else:
            for CurrentTradeTime in listCurrentTime:
                CurrentTradeTime = pd.to_datetime(CurrentTradeTime)
                getOneMa(freq, goodsCode, CurrentTradeTime)

def getMaStdGeneral(dfFreq, mvl):
    dfFreq['maprice_{}'.format(mvl)] = dfFreq['amt'].rolling(mvl).sum() / dfFreq['volume'].rolling(mvl).sum()
    dfFreq['stdprice_{}'.format(mvl)] = dfFreq['close'].rolling(mvl).std()
    dfFreq['stdmux_{}'.format(mvl)] = (dfFreq['close'] - dfFreq['maprice_{}'.format(mvl)]) / dfFreq['stdprice_{}'.format(mvl)]
    dfFreq['highstdmux_{}'.format(mvl)] = (dfFreq['high'] - dfFreq['maprice_{}'.format(mvl)]) / dfFreq['stdprice_{}'.format(mvl)]
    dfFreq['lowstdmux_{}'.format(mvl)] = (dfFreq['low'] - dfFreq['maprice_{}'.format(mvl)]) / dfFreq['stdprice_{}'.format(mvl)]
    return dfFreq[['maprice_{}'.format(mvl),
                    'stdprice_{}'.format(mvl),
                    'stdmux_{}'.format(mvl),
                    'highstdmux_{}'.format(mvl),
                    'lowstdmux_{}'.format(mvl)]][mvl - 1:]