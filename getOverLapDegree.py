from onBar import *
"""
精确地去计算重叠度吧
"""
def getOverLapDegree(freq):
    con = dictFreqCon[freq]
    downLogProgram("检查 CTA{} 重叠度数据".format(freq))
    for goodsCode in dictGoodsName.keys():
        goodsName = dictGoodsName[goodsCode]
        if dictData[freq][dictGoodsName[goodsCode] + '_重叠度表'].shape[0] > 0:
            startTime = dictData[freq][dictGoodsName[goodsCode] + '_重叠度表'].index[-1]
        else:
            startTime = dictData[1][goodsName + '_调整表'].index[0]
        # endTime 为均值表的最后一条数据
        endTime = dictData[freq][dictGoodsName[goodsCode] + '_均值表'].index[-1]
        listCurrentTime = list(dictData[freq][dictGoodsName[goodsCode] + '_均值表'][dictData[freq][dictGoodsName[goodsCode] + '_均值表'].index > startTime].index)
        if endTime - startTime > timedelta(days=5):
            dfMaAll = dictData[freq][dictGoodsName[goodsCode] + '_均值表'].copy()
            # 创建重叠度表格
            dfOverLap = dfMaAll[['goods_code', 'goods_name', 'open', 'high', 'low', 'close']]
            # 获取调整时刻表
            dfAdjustAll = dictGoodsAdj[goodsCode].copy()
            for mvl in mvLenVector:
                dfMa = dfMaAll[max(dfMaAll[dfMaAll.index <= startTime].shape[0] - mvl + 1, 0):]
                dfMa['StdMux高均值_{}'.format(mvl)] = np.nan
                dfMa['重叠度高_{}'.format(mvl)] = np.nan
                dfMa['StdMux低均值_{}'.format(mvl)] = np.nan
                dfMa['重叠度低_{}'.format(mvl)] = np.nan
                dfMa['StdMux收均值_{}'.format(mvl)] = np.nan
                dfMa['重叠度收_{}'.format(mvl)] = np.nan
                dfMa['重叠度高收益_{}'.format(mvl)] = np.nan
                dfMa['重叠度低收益_{}'.format(mvl)] = np.nan
                dfMa['重叠度收收益_{}'.format(mvl)] = np.nan
                dfOverLap['StdMux高均值_{}'.format(mvl)] = np.nan
                dfOverLap['重叠度高_{}'.format(mvl)] = np.nan
                dfOverLap['StdMux低均值_{}'.format(mvl)] = np.nan
                dfOverLap['重叠度低_{}'.format(mvl)] = np.nan
                dfOverLap['StdMux收均值_{}'.format(mvl)] = np.nan
                dfOverLap['重叠度收_{}'.format(mvl)] = np.nan
                dfOverLap['重叠度高收益_{}'.format(mvl)] = np.nan
                dfOverLap['重叠度低收益_{}'.format(mvl)] = np.nan
                dfOverLap['重叠度收收益_{}'.format(mvl)] = np.nan
                dfAdj = dfAdjustAll[(dfAdjustAll['adjdate'] > dfMa.index[0]) & (dfAdjustAll['adjdate'] < endTime)]
                if dfAdj.shape[0] > 0:
                    dfOverLap.update(getOverLapGeneral(dfMa[dfMa.index < dfAdj['adjdate'][0]].copy(), mvl))
                    for each_num in range(dfAdj.shape[0]):
                        loc = dfMa[dfMa.index < dfAdj['adjdate'][each_num]].shape[0]
                        locLeft = max(loc - mvl + 1, 0)
                        dfMa['close'][locLeft:loc] = dfMa['close'][locLeft:loc] + dfAdj['adjinterval'][each_num]
                        dfMa['high'][locLeft:loc] = dfMa['high'][locLeft:loc] + dfAdj['adjinterval'][each_num]
                        dfMa['low'][locLeft:loc] = dfMa['low'][locLeft:loc] + dfAdj['adjinterval'][each_num]
                        if each_num != dfAdj.shape[0] - 1:
                            locBefore = dfMa[dfMa.index < dfAdj['adjdate'][each_num + 1]].shape[0]
                            dfOverLap.update(getOverLapGeneral(dfMa[locLeft:locBefore].copy(), mvl))
                        else:
                            dfOverLap.update(getOverLapGeneral(dfMa[locLeft:].copy(), mvl))
                else:
                    dfOverLap.update(getOverLapGeneral(dfMa.copy(), mvl))
            dfOverLap = dfOverLap[dfOverLap.index > startTime]
            if dfOverLap.shape[0] > 0:
                table = con[dictGoodsName[goodsCode] + '_重叠度表']
                dfInsertMongo(dfOverLap, table, index=True)
            dictData[freq][dictGoodsName[goodsCode] + '_重叠度表'] = dictData[freq][dictGoodsName[goodsCode] + '_重叠度表'].append(dfOverLap).dropna(axis = 1, how = 'all')
        else:
            for CurrentTradeTime in listCurrentTime:
                getOneOverLapDegree(freq, goodsCode, CurrentTradeTime)

def overLapHigh(t, s0, s1, mvl):
    num = mvl // 10
    time_index = s0.index.get_loc(t)
    highstd = s0[time_index - mvl + 1:time_index + 1].sort_values(ascending=False, kind = 'mergesort')[:num]
    high = s1[time_index - mvl + 1:time_index + 1].sort_values(ascending=False, kind = 'mergesort')[:num]
    len_high = len(highstd.index.intersection(high.index))
    if (highstd < 0).all():
        return -100
    else:
        return round(len_high / num, 2)

def overLapLow(t, s0, s1, mvl):
    num = mvl // 10
    time_index = s0.index.get_loc(t)
    lowstd = (s0[time_index - mvl + 1:time_index + 1] * (-1)).sort_values(ascending=False, kind='mergesort')[:num]
    low = (s1[time_index - mvl + 1:time_index + 1] * (-1)).sort_values(ascending=False, kind='mergesort')[:num]
    len_low = len(lowstd.index.intersection(low.index))
    if (lowstd * (-1) > 0).all():
        return -100
    else:
        return round(len_low / num, 2)

def overLapClose(t, s0, s1, mvl):
    num = mvl // 10
    time_index = s0.index.get_loc(t)
    closestd = s0[time_index - mvl + 1:time_index + 1].sort_values(ascending=False, kind='mergesort')[:num]
    close = s1[time_index - mvl + 1:time_index + 1].sort_values(ascending=False, kind='mergesort')[:num]
    len_close = len(closestd.index.intersection(close.index))
    if (s0[time_index - mvl + 1:time_index + 1].sort_index(ascending=False)[:num] < 0).all():
        return -100
    else:
        return round(len_close / num, 2)

def getOverLapGeneral(dfMa, mvl):
    if dfMa[mvl - 1:].shape[0] > 0:
        dfMa['重叠度高_{}'.format(mvl)][mvl - 1:] = pd.Series(dfMa.index)[mvl - 1:].apply(overLapHigh, args = (dfMa['highstdmux_{}'.format(mvl)], dfMa['high'], mvl))
        dfMa['重叠度低_{}'.format(mvl)][mvl - 1:] = pd.Series(dfMa.index)[mvl - 1:].apply(overLapLow, args = (dfMa['lowstdmux_{}'.format(mvl)], dfMa['low'], mvl))
        dfMa['重叠度收_{}'.format(mvl)][mvl - 1:] = pd.Series(dfMa.index)[mvl - 1:].apply(overLapClose, args = (dfMa['stdmux_{}'.format(mvl)], dfMa['close'], mvl))
    return dfMa[['重叠度高_{}'.format(mvl), '重叠度低_{}'.format(mvl), '重叠度收_{}'.format(mvl)]][mvl - 1:]
