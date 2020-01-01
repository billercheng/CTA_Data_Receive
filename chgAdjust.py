from parameter import *

def checkChg():
    for goodsCode in listGoods:
        dfChgData = pd.read_csv(r'.\chg_data' + '\\' + goodsCode.upper() + ' chg_data.csv', encoding='gbk',
                                parse_dates=['adjdate'])
        dfChgData = dfChgData.drop(['id'], axis=1)
        dfChgData = dfChgData.reset_index(drop=True)
        if goodsCode.split('.')[1] in ['CZC', 'CFE']:
            dfChgData['goods_code'] = dfChgData['stock']
        else:
            dfChgData['goods_code'] = dfChgData['stock'].apply(
                lambda x: x.split('.')[0].lower() + '.' + x.split('.')[1])
        dfChgData = dfChgData[['goods_code', 'goods_name', 'adjdate', 'adjinterval']]
        dfChgData = dfChgData[1:-1]
        dfChgData = dfChgData.set_index('goods_code')
        dfChgData['adjdate'] = pd.to_datetime(dfChgData['adjdate']) + timedelta(hours=17)
        dictGoodsAdj[goodsCode] = dfChgData.copy()  # 切换合约表
        dictGoodsInstrument[goodsCode] = dictGoodsAdj[goodsCode].index[-1].split('.')[0]  # 主力合约
        dictGoodsVolume[goodsCode] = {'volume': 0, 'amt': 0}  # 成交量与成交额，因为会有切换合约的情况