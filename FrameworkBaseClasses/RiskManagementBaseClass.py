from FrameworkBaseClasses.ProcessBaseClass import ProcessBaseClass
from assets import constants as Constant

import mysql.connector
from datetime import datetime, timezone


class RiskManagementBaseClass(ProcessBaseClass):
    def __init__(self):
        # print("Risk Management Base Class Constructor")
        self.ProcessName = "Risk Management Process"
        self.ObjectTypeValidationArr = {
            'SystemVariables': ['AlgorithmId', 'CurrentPrice', 'CurrentAccountBalance', 'CurrentPortfolioValue', 'TradingState'],
            'DatabaseDetails': ['ServerName', 'DatabaseName', 'UserName', 'Password'],
            'Indicators': ['BB', 'RSI', 'SMA', 'EMA', 'EMA_RETEST', 'TimeStamp', 'COC'],
            'ExchangeDetails': ['ExchangeName', 'ApiKey', 'ApiSecret']
        }
        super().__init__()

    # region Functions used to retrieve information from the database
    def getAlgorithmTradingState(self):
        # print("get Algorithm Trading State")
        QueryStr = """Select * From AlgorithmConfiguration Where AlgorithmName = %s"""

        QueryData = (
            self.CurrentSystemVariables['AlgorithmId'],
        )

        AlgorithmConfigurationObjArr = self.templateDatabaseRetriever(QueryStr, QueryData, "getAlgorithmTradingState")
        if AlgorithmConfigurationObjArr is None or len(AlgorithmConfigurationObjArr) != 1:
            return
        AlgorithmTradingState = AlgorithmConfigurationObjArr[0][Constant.ALGORITHM_CONFIGURATION_TRADING_STATE_INDEX]
        
        if AlgorithmTradingState == 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = 'Manual Halt'
            return
        elif AlgorithmTradingState != 'Manual Halt' and \
                self.CurrentSystemVariables['TradingState'] == 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = AlgorithmTradingState

        # region Funding Time related checks for bitmex
        if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BITMEX_EXCHANGE_ID:
            CurrentDateTimeObj = datetime.now(timezone.utc).replace(tzinfo=None)
            FundingTimeObjArr = [
                datetime.utcnow().replace(hour=Constant.FIRST_FUNDING_HOUR, minute=0),
                datetime.utcnow().replace(hour=Constant.SECOND_FUNDING_HOUR, minute=0),
                datetime.utcnow().replace(hour=Constant.THIRD_FUNDING_HOUR, minute=0),
            ]

            isFundingTimeBool = False
            for FundingTimeObj in FundingTimeObjArr:
                if CurrentDateTimeObj < FundingTimeObj:
                    TimeToFunding = FundingTimeObj - CurrentDateTimeObj
                    if TimeToFunding.total_seconds() <= Constant.MARKET_DEAD_STOP_SECONDS_TO_FUNDING:
                        self.CurrentSystemVariables['TradingState'] = 'Market Dead Stop'
                        isFundingTimeBool = True
                    elif TimeToFunding.total_seconds() <= Constant.MARKET_HALT_SECONDS_TO_FUNDING:
                        self.CurrentSystemVariables['TradingState'] = 'Market Halt'
                        isFundingTimeBool = True
                    break
            if not isFundingTimeBool and (AlgorithmTradingState == 'Market Dead Stop'
                                          or AlgorithmTradingState == 'Market Halt'):
                self.CurrentSystemVariables['TradingState'] = 'Active'
        # endregion

        if AlgorithmTradingState != self.CurrentSystemVariables['TradingState']:
            if self.CurrentSystemVariables['TradingState'] is None:
                self.CurrentSystemVariables['TradingState'] = AlgorithmTradingState
            self.setAlgorithmTradingState(self.CurrentSystemVariables['TradingState'])
    # endregion

    # region Functions used to log process successes and failures as system executes
    # also to update configuration entries
    def setAlgorithmTradingState(self, TradingStateStr):
        # print("set Algorithm Trading State")
        QueryStr = """Update AlgorithmConfiguration Set TradingState = %s Where AlgorithmName = %s"""

        QueryData = (
            TradingStateStr,
            self.CurrentSystemVariables['AlgorithmId'],
        )

        self.templateDatabaseLogger(QueryStr, QueryData, "setAlgorithmTradingState")
    # endregion
