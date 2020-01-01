from py_ctp.ctp_trade import Trade
from parameter import *

class TdApi:
    def __init__(self, userid, password, brokerid, RegisterFront, product_info, app_id, auth_code):
        # 创建 Trade 的类
        self.t = Trade()
        # 建立好 帐户类
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        self.product_info = product_info
        self.auth_code = auth_code
        self.app_id = app_id
        api = self.t.CreateApi()
        spi = self.t.CreateSpi()
        self.t.RegisterSpi(spi)
        self.t.OnFrontConnected = self.onFrontConnected  # 交易服务器登陆相应
        self.t.OnFrontDisconnected = self.onFrontDisconnected  # 交易服务器断开连接的情况
        self.t.OnRspAuthenticate = self.onRspAuthenticate  # 申请码检验
        self.t.OnRspUserLogin = self.onRspUserLogin  # 用户登陆
        self.t.OnRspUserLogout = self.onRspUserLogout  # 用户登出
        self.t.OnRspQryDepthMarketData = self.onRspQryDepthMarketData  # 查询涨跌停
        self.t.OnRtnInstrumentStatus = self.onRtnInstrumentStatus
        self.t.OnRspQryInstrument = self.onRspQryInstrument
        self.t.OnErrRtnOrderInsert = self.onErrRtnOrderInsert
        self.t.OnRspSettlementInfoConfirm = self.onRspSettlementInfoConfirm  # 结算单确认，显示登陆日期
        self.t.OnRtnOrder = self.onRtnOrder
        self.t.OnRtnTrade = self.onRtnTrade
        self.t.RegCB()
        self.t.RegisterFront(RegisterFront)
        self.t.Init()
        self.isLogin = False

    def onFrontConnected(self):  # 服务器连接成功能，进行注册码注册操作
        downLogProgram('交易服务器连接成功')
        self.t.ReqAuthenticate(self.brokerid, self.userid, self.product_info, self.auth_code, self.app_id)

    def onFrontDisconnected(self, n):  # 交易服务器断开连接
        downLogProgram('交易服务器连接断开')
        self.isLogin = False

    def onRspAuthenticate(self, pRspAuthenticateField: CThostFtdcRspAuthenticateField,
                          pRspInfo: CThostFtdcRspInfoField,
                          nRequestID: int, bIsLast: bool):  # 注册码成功后，进行登陆操作
        downLogProgram('auth：{0}:{1}'.format(pRspInfo.getErrorID(), pRspInfo.getErrorMsg()))
        if pRspInfo.getErrorMsg() == '正确' or pRspInfo.getErrorMsg() == 'CTP:认证码错误，尽快获取正确的认证码。当前系统或者用户豁免终端认证，可以登录':
            self.t.ReqUserLogin(BrokerID=self.brokerid, UserID=self.userid, Password=self.password, UserProductInfo=self.product_info)
        elif pRspInfo.getErrorMsg() == 'CTP:前置不活跃':
            threading.Timer(60, self.t.ReqAuthenticate, [self.brokerid, self.userid, self.product_info, self.auth_code, self.app_id]).start()

    def onRspUserLogin(self, data, error, n, last):
        """登陆回报"""
        if error.getErrorID() == 0:
            self.Investor = data.getUserID()
            self.BrokerID = data.getBrokerID()
            log = self.Investor + ' 交易服务器登陆成功'
            downLogProgram(log)
            self.t.ReqSettlementInfoConfirm(self.BrokerID, self.Investor)  # 对账单确认
        else:
            log = '交易服务器登陆回报，错误代码：' + str(error.getErrorID()) + \
                  ',   错误信息：' + str(error.getErrorMsg())
            downLogProgram(log)
            self.isLogin = False
            if error.getErrorMsg() == 'CTP:客户端未认证':  # 过 60 秒后，重新登陆一次
                threading.Timer(60, self.t.ReqUserLogin, ['', self.brokerid, self.userid, self.password, self.product_info]).start()

    def onRspUserLogout(self, data, error, n, last):
        if error.getErrorID() == 0:
            log = '交易服务器登出成功'
            downLogProgram(log)
        else:
            log = '交易服务器登出回报，错误代码：' + str(error.getErrorID()) + \
                  ',   错误信息：' + str(error.getErrorMsg())
            downLogProgram(log)
            if error.getErrorMsg() == 'CTP:客户端未认证':  # 过 60 秒后，重新登陆一次
                threading.Timer(60, self.t.ReqUserLogin, ['', self.brokerid, self.userid, self.password, self.product_info]).start()

    def onRspQryDepthMarketData(self, data, error, n, last): #获取tick数据操作
        # region  将 合约号 去除所有的数字，直接变成一个 icon 的操作
        goodsIcon = filter(lambda x: x.isalpha(), data.getInstrumentID())
        goodsIcon = ''.join(list(goodsIcon))
        # endregion
        event = Event(type_=EVENT_INSTRUMENT)
        event.dict_['InstrumentID'] = data.getInstrumentID()  # 合约号
        event.dict_['ProductID'] = goodsIcon
        event.dict_['OpenInterest'] = data.getOpenInterest()  # 持仓量
        event.dict_['LastPrice'] = data.getLastPrice()  # 获取收盘价
        event.dict_['last'] = last  # 是否为最后一笔
        ee.put(event)

    def onRspSettlementInfoConfirm(self, data, error, n, last):
        """确认结算信息回报"""
        self.isLogin = True
        downLogProgram('账号：{}, 日期：{}, 时间：{} 交易账户已登陆成功'.format(data.getInvestorID(), data.getConfirmDate(), data.getConfirmTime()))

    def onRtnInstrumentStatus(self, data):
        pass

    def onRspQryInstrument(self, data, error, n, last):
        pass

    def onRtnOrder(self, data):
        # 常规报单事件
        pass

    def onRtnTrade(self, data):
        """成交回报"""
        pass

    def onErrRtnOrderInsert(self, data, error):
        pass




















