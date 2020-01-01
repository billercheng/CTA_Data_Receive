from parameter import *
from WindPy import w

"""
1:当1分钟数据合成后，在合成多分钟数据时，进行相对应多分钟数据的计算均值与重叠度操作
"""

def onBar(dictTemp):
    dictTemp = insertDbChg(dictTemp).copy()
    table = dictFreqCon[1][dictGoodsName[dictTemp['theCode']] + '_调整表']
    dictTempTemp = dictTemp.copy()
    dictTempTemp.pop('theCode')
    table.insert_one(dictTempTemp)
    tradeTime = dictTemp['trade_time']
    goodsCode = dictTemp['theCode']
    goodsName = dictGoodsName[goodsCode]
    goodsInstrument = dictTemp['goods_code']
    # 凭什么认为是完整的呢？
    dictData[1][goodsName + '_调整表'].loc[tradeTime] = [dictTemp[x] for x in listMin]
    for freq in listFreq:
        if tradeTime.time() in dictFreqGoodsClose[freq][goodsCode]:
            downLogBarDeal("品种：{} 频段：{} 时间为：{} 数据处理".
                               format(goodsInstrument, freq, tradeTime.strftime("%Y-%m-%d %H:%M:%S")), freq)
            # region 对新的bar数据进行 均值表 重叠度表 周交易明细表计算
            if tradeTime.time() == dictFreqGoodsClose[freq][goodsCode][-1]:
                tBehind = dictFreqGoodsClose[freq][goodsCode][-1]
                tFront = dictFreqGoodsClose[freq][goodsCode][-2]
                minDelay = dictFreqGoodsClose[1][goodsCode].index(tBehind) - dictFreqGoodsClose[1][goodsCode].index(tFront)
                df = dictData[1][goodsName + '_调整表'][minDelay * (-1):].copy()
            else:
                df = dictData[1][goodsName + '_调整表'][freq * (-1):].copy()
            theDict = {}
            theDict['goods_code'] = goodsInstrument
            theDict['goods_name'] = goodsName
            theDict['open'] = df['open'][0]
            theDict['high'] = df['high'].max()
            theDict['low'] = df['low'].min()
            theDict['close'] = df['close'][-1]
            theDict['volume'] = df['volume'].sum()
            theDict['amt'] = df['amt'].sum()
            theDict['oi'] = df['oi'][-1]
            downLogBarDeal("品种：{} 频段：{} 时间为：{} 进行频段数据合成".
                               format(goodsInstrument, freq, tradeTime.strftime("%Y-%m-%d %H:%M:%S")), freq)
            downLogBarDeal(str(theDict), freq)
            dictData[freq][goodsName + '_调整表'].loc[tradeTime] = [theDict[x] for x in listMin]
            theDict['trade_time'] = tradeTime
            theDict = insertDbChg(theDict)
            table = dictFreqCon[freq][goodsName + '_调整表']
            table.insert_one(theDict)
            downLogBarDeal("增加对应时间的均值数据", freq)
            getOneMa(freq, goodsCode, tradeTime)
            downLogBarDeal("增加对应时间的重叠度数据", freq)
            getOneOverLapDegree(freq, goodsCode, tradeTime)

#region 均值表
def getOneMa(freq, goodsCode, CurrentTradeTime):
    goodsName = dictGoodsName[goodsCode]
    dfFreqAll = dictData[freq][goodsName + '_调整表'].copy()
    dfFreqAll = dfFreqAll[dfFreqAll.index <= CurrentTradeTime]
    theDict = {}
    theDict['goods_code'] = dfFreqAll['goods_code'][-1]
    theDict['goods_name'] = dfFreqAll['goods_name'][-1]
    theDict['open'] = dfFreqAll['open'][-1]
    theDict['high'] = dfFreqAll['high'][-1]
    theDict['low'] = dfFreqAll['low'][-1]
    theDict['close'] = dfFreqAll['close'][-1]
    dfAdjustAll = dictGoodsAdj[goodsCode].copy()
    dfAdj = dfAdjustAll[
        (dfAdjustAll['adjdate'] > dfFreqAll.index[0]) & (dfAdjustAll['adjdate'] < dfFreqAll.index[-1])]
    for eachNum in range(dfAdj.shape[0]):
        loc = dfFreqAll[dfFreqAll.index < dfAdj['adjdate'][eachNum]].shape[0]  # 调整时刻位置
        dfFreqAll['close'][:loc] = dfFreqAll['close'][:loc] + \
                                   dfAdj['adjinterval'][eachNum]
    dfFreqAll['amt'] = dfFreqAll['close'] * dfFreqAll['volume']
    for eachMvl in mvLenVector:
        dfFreq = dfFreqAll[(-1) * eachMvl:]
        theDict['maprice_{}'.format(eachMvl)] = dfFreq['amt'].sum() / dfFreq['volume'].sum()
        theDict['stdprice_{}'.format(eachMvl)] = dfFreq['close'].std()
        theDict['stdmux_{}'.format(eachMvl)] = (dfFreq['close'][-1] - theDict['maprice_{}'.format(eachMvl)]) / theDict[
            'stdprice_{}'.format(eachMvl)]
        theDict['highstdmux_{}'.format(eachMvl)] = (dfFreq['high'][-1] - theDict['maprice_{}'.format(eachMvl)]) / theDict[
            'stdprice_{}'.format(eachMvl)]
        theDict['lowstdmux_{}'.format(eachMvl)] = (dfFreq['low'][-1] - theDict['maprice_{}'.format(eachMvl)]) / theDict[
            'stdprice_{}'.format(eachMvl)]
    dictData[freq][goodsName + '_均值表'].loc[CurrentTradeTime] = [theDict[x] for x in listMa]
    theDict['trade_time'] = CurrentTradeTime
    theDict = insertDbChg(theDict)
    table = dictFreqCon[freq][goodsName + '_均值表']
    table.insert_one(theDict)
#endregion

#region 重叠度表
def getOneOverLapDegree(freq, goodsCode, CurrentTradeTime):
    goodsName = dictGoodsName[goodsCode]
    dfMaALL = dictData[freq][goodsName + '_均值表'].copy()
    dfMaALL = dfMaALL[dfMaALL.index <= CurrentTradeTime]
    theDict = {}
    theDict['goods_code'] = dfMaALL['goods_code'][-1]
    theDict['goods_name'] = dfMaALL['goods_name'][-1]
    theDict['open'] = dfMaALL['open'][-1]
    theDict['high'] = dfMaALL['high'][-1]
    theDict['low'] = dfMaALL['low'][-1]
    theDict['close'] = dfMaALL['close'][-1]
    dfAdjustAll = dictGoodsAdj[goodsCode].copy()
    dfAdj = dfAdjustAll[
        (dfAdjustAll['adjdate'] > dfMaALL.index[0]) & (dfAdjustAll['adjdate'] < dfMaALL.index[-1])]
    for eachNum in range(dfAdj.shape[0]):
        loc = dfMaALL[dfMaALL.index < dfAdj['adjdate'][eachNum]].shape[0]
        dfMaALL['close'][:loc] = dfMaALL['close'][:loc] + dfAdj['adjinterval'][eachNum]
        dfMaALL['high'][:loc] = dfMaALL['high'][:loc] + dfAdj['adjinterval'][eachNum]
        dfMaALL['low'][:loc] = dfMaALL['low'][:loc] + dfAdj['adjinterval'][eachNum]
    for eachMvl in mvLenVector:
        dfMa = dfMaALL[(-1) * eachMvl:].copy()
        num = eachMvl // 10
        HighPriceSortedTab = dfMa['high'].nlargest(num, keep='last')
        HighStdSortedTab = dfMa['highstdmux_{}'.format(eachMvl)].nlargest(num, keep='last')
        if (HighStdSortedTab < 0).all():
            theDict['重叠度高_{}'.format(eachMvl)] = -100
        else:
            theDict['重叠度高_{}'.format(eachMvl)] = len(HighPriceSortedTab.index.intersection(HighStdSortedTab.index)) / num
        LowPriceSortedTab = dfMa['low'].nsmallest(num, keep='last')
        LowStdSortedTab = dfMa['lowstdmux_{}'.format(eachMvl)].nsmallest(num, keep='last')
        if (LowStdSortedTab > 0).all():
            theDict['重叠度低_{}'.format(eachMvl)] = -100
        else:
            theDict['重叠度低_{}'.format(eachMvl)] = len(LowPriceSortedTab.index.intersection(LowStdSortedTab.index)) / num
        ClosePriceSortedTab = dfMa['close'].nlargest(num, keep='last')
        CloseStdSortedTab = dfMa['stdmux_{}'.format(eachMvl)].nlargest(num, keep='last')
        if (CloseStdSortedTab < 0).all():
            theDict['重叠度收_{}'.format(eachMvl)] = -100
        else:
            theDict['重叠度收_{}'.format(eachMvl)] = len(ClosePriceSortedTab.index.intersection(CloseStdSortedTab.index)) / num
    dictData[freq][goodsName + '_重叠度表'].loc[CurrentTradeTime] = [theDict[x] for x in listOverLap]
    theDict['trade_time'] = CurrentTradeTime
    table = dictFreqCon[freq][goodsName + '_重叠度表']
    table.insert_one(theDict)
#endregion