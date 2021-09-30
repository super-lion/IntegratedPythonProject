from FrameworkBaseClasses.RiskManagementBaseClass import RiskManagementBaseClass
from assets import constants as Constant

from datetime import datetime


class RiskManagementClass(RiskManagementBaseClass):
    def __init__(self):
        # print("Risk Management Class Constructor")
        super().__init__()

    def initiateExecution(self):

        # region Getting up to date algorithm trading state
        AlgorithmNameStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]
        QueryStr = """Select * From AlgorithmConfiguration Where AlgorithmName = %s"""

        QueryData = (
            AlgorithmNameStr,
        )

        AlgorithmConfigurationObjArr = self.templateDatabaseRetriever(QueryStr, QueryData,
                                                                      "determineTradingStateBbRsiImproved")
        if AlgorithmConfigurationObjArr is None or len(AlgorithmConfigurationObjArr) != 1:
            return
        AlgorithmTradingState = AlgorithmConfigurationObjArr[0][Constant.ALGORITHM_CONFIGURATION_TRADING_STATE_INDEX]
        # endregion

        self.monitorTradeFrequency(AlgorithmTradingState)
        self.monitorCocIndicator()
        if AlgorithmNameStr == Constant.BB_RSI_ALGORITHM_V2:
            self.determineTradingStateBbRsiImproved(AlgorithmTradingState)
        elif AlgorithmNameStr == Constant.BB_RSI_ALGORITHM_V3:
            self.determineTradingStateBbRsiV3(AlgorithmTradingState)
        else:
            self.getAlgorithmTradingState()

    def determineTradingStateBbRsiImproved(self, AlgorithmTradingState):
        # print("get Algorithm Trading State")
        AlgorithmRiskManagementLimitInt = 12

        if AlgorithmTradingState == 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = 'Manual Halt'
            return
        elif AlgorithmTradingState != 'Manual Halt' and \
                self.CurrentSystemVariables['TradingState'] == 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = AlgorithmTradingState

        if self.IndicatorsObj['COC']['OrderCount'] > AlgorithmRiskManagementLimitInt and AlgorithmTradingState != 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = 'Market Dead Stop'
        elif self.IndicatorsObj['COC']['OrderCount'] <= AlgorithmRiskManagementLimitInt and AlgorithmTradingState == 'Market Dead Stop':
            self.CurrentSystemVariables['TradingState'] = 'Active'

        if AlgorithmTradingState != self.CurrentSystemVariables['TradingState']:
            if self.CurrentSystemVariables['TradingState'] is None:
                self.CurrentSystemVariables['TradingState'] = AlgorithmTradingState
            self.setAlgorithmTradingState(self.CurrentSystemVariables['TradingState'])

    def determineTradingStateBbRsiV3(self, AlgorithmTradingState):
        # print("get Algorithm Trading State")
        AlgorithmRiskManagementLimitInt = 12

        if AlgorithmTradingState == 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = 'Manual Halt'
            return
        elif AlgorithmTradingState != 'Manual Halt' and \
                self.CurrentSystemVariables['TradingState'] == 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = AlgorithmTradingState

        if self.IndicatorsObj['COC']['OrderCount'] > AlgorithmRiskManagementLimitInt and AlgorithmTradingState != 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = 'Reverse'
        elif self.IndicatorsObj['COC']['OrderCount'] <= AlgorithmRiskManagementLimitInt and AlgorithmTradingState == 'Reverse':
            self.CurrentSystemVariables['TradingState'] = 'Active'

        if AlgorithmTradingState != self.CurrentSystemVariables['TradingState']:
            if self.CurrentSystemVariables['TradingState'] is None:
                self.CurrentSystemVariables['TradingState'] = AlgorithmTradingState
            self.setAlgorithmTradingState(self.CurrentSystemVariables['TradingState'])

    def monitorTradeFrequency(self, AlgorithmTradingState):
        if AlgorithmTradingState == 'Manual Halt':
            return

        for TradeLimitSpecArr in Constant.TRADE_LIMITING_SPECIFICATION_ARR:
            TimeSpanInt = TradeLimitSpecArr[0]
            TradeLimit = TradeLimitSpecArr[1]
            ExecutedTradeArr = self.getMyTrades(TimeSpanInt)
            if len(ExecutedTradeArr) > TradeLimit:
                self.setAlgorithmTradingState('Manual Halt')
                self.CurrentSystemVariables['TradingState'] = 'Manual Halt'
                self.createProcessExecutionLog(self.ProcessName, datetime.utcnow(), "Process Update: Set algorithm trading state to Manual Halt due to violation of trade limit: " + str(TradeLimitSpecArr))
                break
        return

    def monitorCocIndicator(self):
        if self.CurrentSystemVariables['CurrentAccountPositionSize'] == 0 and self.IndicatorsObj['COC']['OrderCount'] != 0:
            self.IndicatorsObj['COC']['OrderCount'] = 0
    # endregion
