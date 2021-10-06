from FrameworkBaseClasses.ProcessBaseClass import ProcessBaseClass
from assets import environments as EnvironmentDetails
from assets import constants as Constant
from assets import ProjectFunctions
import sys as SystemObj

import ccxt
import mysql.connector
import threading
import traceback
import time
from datetime import datetime


class ManagerBaseClass(ProcessBaseClass):
    ProcessName = 'Manager Process'
    ThreadInstantiationArr = []
    SystemVariablesObj = {
        'AlgorithmId': None,
        'CurrentPrice': None,
        'CurrentAccountBalance': None,
        'CurrentPortfolioValue': None,
        'SystemState': None,
        'TradingState': None
    }

    IndicatorGenerationObj = None
    RiskManagementObj = None
    TraderObj = None

    def __init__(self):
        # print("Manager Base Class Constructor")
        # print(dir(ccxt.binance()))  # Used to display all the api endpoints available on the exchange
        self.requestProjectInitiationState()

    def startProcessThreading(self):
        # print("start Process Threading")
        for ProcessInfoObj in self.ThreadInstantiationArr:
            threading.Thread(target=self.executeEachTimeInterval,
                             args=(ProcessInfoObj['ProcessObj'], ProcessInfoObj['IntervalInt'])).start()

    def executeEachTimeInterval(self, ProcessObj: ProcessBaseClass, IntervalInt):
        # print("execute Each Time Interval")
        StartingTimeInt = time.time()
        while True:  # change this to work based on system state
            # self.createProcessExecutionLog(ProcessObj.ProcessName, datetime.utcnow(), "Starting Process Execution")

            for iterator in range(0, Constant.RETRY_LIMIT):
                try:
                    ProcessObj.initiateExecution()
                    # self.createProcessExecutionLog(ProcessObj.ProcessName, datetime.utcnow(), "Process Executed Successfully")
                    break
                except Exception as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT-1:
                        continue
                    self.createProcessExecutionLog(ProcessObj.ProcessName, datetime.utcnow(),
                                                   "Process Failed: " + str(ErrorMessage) + "\n" + traceback.format_exc())

            if time.time() - StartingTimeInt < IntervalInt:
                SleepingTimeFloat = IntervalInt - (time.time() - StartingTimeInt)
                if SleepingTimeFloat > 0:
                    time.sleep(SleepingTimeFloat)
                StartingTimeInt = time.time()
            else:
                self.createProcessExecutionLog(
                    ProcessObj.ProcessName,
                    datetime.utcnow(),
                    "Process took " + str(time.time() - StartingTimeInt) + " seconds to execute")
                StartingTimeInt = time.time()

    def requestProjectAlgorithmSelection(self):
        AlgorithmNameArr = self.getAlgorithmNames()
        AlgorithmOptionsStr = ""
        for AlgorithmNameIndexInt in range(0, len(AlgorithmNameArr)):
            AlgorithmOptionsStr += str(AlgorithmNameIndexInt+1) + ". " + AlgorithmNameArr[AlgorithmNameIndexInt][0] + '\n'
        SelectedAlgorithmNameInputStr = input("Please select an algorithm:\n" + AlgorithmOptionsStr + "Input: ")

        if 0 <= int(SelectedAlgorithmNameInputStr) <= len(AlgorithmNameArr):
            self.AlgorithmConfigurationObj = \
                self.getAlgorithmConfigurationObj(AlgorithmNameArr[int(SelectedAlgorithmNameInputStr) - 1][0])
            self.SystemVariablesObj['AlgorithmId'] = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]
            self.SystemVariablesObj['TradingState'] = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_STATE_INDEX]

            if not self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_WEBHOOK]:
                self.initializeSystemData()
        self.setExchangeConnection()

    def requestProjectInitiationState(self):
        # print("request Project Initiation State")
        SystemStateInputStr = input("Please provide the system state:\n1. Active\n2. Passive\nInput: ")
        if SystemStateInputStr.strip() == '1':
            self.setSystemState("Active")
        elif SystemStateInputStr.strip() == '2':
            self.setSystemState("Passive")
        else:
            print("Invalid selection")
            SystemObj.exit()

    def setSystemState(self, SystemStateStr):
        # print("set System State")
        self.SystemVariablesObj['SystemState'] = SystemStateStr
        if SystemStateStr == 'Active':
            self.DatabaseConnectionDetails['ServerName'] = EnvironmentDetails.MULTIPLE_ALGORITHM_HOST
            self.DatabaseConnectionDetails['DatabaseName'] = EnvironmentDetails.MULTIPLE_ALGORITHM_DATABASE
            self.DatabaseConnectionDetails['UserName'] = EnvironmentDetails.MULTIPLE_ALGORITHM_USER
            self.DatabaseConnectionDetails['Password'] = EnvironmentDetails.MULTIPLE_ALGORITHM_PASSWORD

            self.requestProjectAlgorithmSelection()

        elif SystemStateStr == 'Testing':
            pass

        elif SystemStateStr == 'Backtesting':
            pass

        elif SystemStateStr == 'Passive':
            self.DatabaseConnectionDetails['ServerName'] = EnvironmentDetails.MULTIPLE_ALGORITHM_HOST
            self.DatabaseConnectionDetails['DatabaseName'] = EnvironmentDetails.MULTIPLE_ALGORITHM_DATABASE
            self.DatabaseConnectionDetails['UserName'] = EnvironmentDetails.MULTIPLE_ALGORITHM_USER
            self.DatabaseConnectionDetails['Password'] = EnvironmentDetails.MULTIPLE_ALGORITHM_PASSWORD

    def authenticateWebhookRequest(self, PayloadObj):
        ResultObj = self.getWebhookAlgorithmConfigurationObj(PayloadObj['ApiSecret'], PayloadObj['ApiKey'])
        if len(ResultObj) < 1:
            return False
        self.AlgorithmConfigurationObj = ResultObj[0]

        self.SystemVariablesObj['AlgorithmId'] = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]
        self.SystemVariablesObj['TradingState'] = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_STATE_INDEX]

        self.setExchangeConnection()
        if self.AlgorithmConfigurationObj is None:
            return False
        return True

    def setExchangeConnection(self):
        # print("set Exchange Connection")
        self.ExchangeConnectionDetails['ExchangeName'] = \
            self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EXCHANGE_NAME_INDEX]
        self.ExchangeConnectionDetails['ApiKey'] = \
            self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_API_KEY_INDEX]
        self.ExchangeConnectionDetails['ApiSecret'] = \
            self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_API_SECRET_INDEX]
        if self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EXCHANGE_NAME_INDEX] == \
                Constant.BITMEX_EXCHANGE_ID:
            for iterator in range(0, Constant.RETRY_LIMIT):
                try:
                    ExchangeClassObj = getattr(ccxt, self.ExchangeConnectionDetails['ExchangeName'])
                    self.ExchangeConnectionObj = ExchangeClassObj({
                        'apiKey': self.ExchangeConnectionDetails['ApiKey'],
                        'secret': self.ExchangeConnectionDetails['ApiSecret'],
                        'timeout': 30000,
                        'enableRateLimit': True,
                        'symbols': [
                            self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                        ]
                    })
                    break

                except Exception as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT-1:
                        continue
                    print("Something went wrong when setting up Exchange connection: " + str(ErrorMessage))
        elif self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EXCHANGE_NAME_INDEX] == \
                Constant.BINANCE_EXCHANGE_ID:
            for iterator in range(0, Constant.RETRY_LIMIT):
                try:
                    ExchangeClassObj = getattr(ccxt, self.ExchangeConnectionDetails['ExchangeName'])
                    self.ExchangeConnectionObj = ExchangeClassObj({
                        'apiKey': self.ExchangeConnectionDetails['ApiKey'],
                        'secret': self.ExchangeConnectionDetails['ApiSecret'],
                        'timeout': 30000,
                        'enableRateLimit': True,
                        'symbols': [
                            self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                        ],
                        'options': {
                            'defaultType': 'margin'
                        }
                    })
                    break
                except Exception as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT-1:
                        continue
                    print("Something went wrong when setting up Exchange connection for Binance: " + str(ErrorMessage))
                    SystemObj.exit()
        else:
            print("Invalid Exchange Name Selection")
            SystemObj.exit()

        self.ExchangeConnectionObj.load_markets()

    # region Functions that need to be overwritten in the child class
    # This function is to be overwritten by the child class
    def initializeSystemData(self):
        pass
    # endregion

    # region Functions used to retrieve information from the exchange
    def getCurrentPrice(self):
        TradingPairSymbolStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                self.SystemVariablesObj['CurrentPrice'] = self.ExchangeConnectionObj.fetch_ticker(TradingPairSymbolStr)['bid']
                break
            except Exception as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                # Please create a log table and a log function for exchange related retrievals.
                # We will only log errors in this table
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    " self.ExchangeConnectionObj.fetch_ticker(" + TradingPairSymbolStr + ")['bid']", ErrorMessage
                )

    def getCurrentBalance(self):
        TradingPairSymbolStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
        PairSplitIndexInt = TradingPairSymbolStr.find('/')
        MarginBaseCurrencyStr = TradingPairSymbolStr[PairSplitIndexInt+1:]
        BalanceObj = None
        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                BalanceObj = self.ExchangeConnectionObj.fetch_balance()
                break
            except ccxt.NetworkError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "fetch_balance()",
                    "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "fetch_balance()",
                    "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "fetch_balance()",
                    "OtherError: " + str(ErrorMessage)
                )

        if BalanceObj is None:
            return

        if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
            self.SystemVariablesObj['CurrentPortfolioValue'] = float(BalanceObj['info']['totalNetAssetOfBtc']) * self.SystemVariablesObj['CurrentPrice']
            for AssetObj in BalanceObj['info']['userAssets']:
                if AssetObj['asset'] == MarginBaseCurrencyStr:
                    self.SystemVariablesObj['CurrentAccountBalance'] = AssetObj['free']
                    return
        elif self.ExchangeConnectionDetails['ExchangeName'] == Constant.BITMEX_EXCHANGE_ID:
            self.SystemVariablesObj['CurrentAccountBalance'] = BalanceObj['free']['BTC']
            self.SystemVariablesObj['CurrentPortfolioValue'] = BalanceObj['total']['BTC']

    def getCurrentPosition(self):
        TradingPairSymbolStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
        PairSplitIndexInt = TradingPairSymbolStr.find('/')
        MarginTradingCurrencyStr = TradingPairSymbolStr[0:PairSplitIndexInt]
        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                    BinanceAssetObjArr = self.ExchangeConnectionObj.fetchBalance()['info']['userAssets']
                    for BinanceAssetObj in BinanceAssetObjArr:
                        if BinanceAssetObj['asset'] == MarginTradingCurrencyStr:
                            if float(BinanceAssetObj['netAsset']) * float(self.SystemVariablesObj['CurrentPrice']) > 10:
                                self.SystemVariablesObj['CurrentAccountPositionSize'] = ProjectFunctions.truncateFloat(abs(float(BinanceAssetObj['netAsset'])), 6)
                            elif float(BinanceAssetObj['netAsset']) * float(self.SystemVariablesObj['CurrentPrice']) < -10:
                                self.SystemVariablesObj['CurrentAccountPositionSize'] = ProjectFunctions.truncateFloat(-abs(float(BinanceAssetObj['netAsset'])), 6)
                            else:
                                self.SystemVariablesObj['CurrentAccountPositionSize'] = 0
                else:
                    CurrentPositionObj = self.ExchangeConnectionObj.private_get_position()
                    self.SystemVariablesObj['CurrentAccountPositionSize'] = CurrentPositionObj[0]['currentQty']
                return True
            except ccxt.NetworkError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "private_get_position()",
                    "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "private_get_position()",
                    "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "private_get_position()",
                    "OtherError: " + str(ErrorMessage)
                )
        return False
    # endregion

    # region Function used to retrieve algorithm configurations from database
    def getAlgorithmNames(self):
        # print("get Algorithm Names")
        QueryStr = """Select AlgorithmName From AlgorithmConfiguration"""

        QueryData = (
        )

        AlgorithmNameArr = self.templateDatabaseRetriever(QueryStr, QueryData, "getAlgorithmNames")
        if AlgorithmNameArr is None:
            return
        return AlgorithmNameArr

    def getAlgorithmConfigurationObj(self, AlgorithmConfigurationNameStr=None, AlgorithmConfigurationId=None):
        # print("get Algorithm Configuration Object")
        if AlgorithmConfigurationNameStr is None and AlgorithmConfigurationId is None:
            return
        elif AlgorithmConfigurationId is None:
            QueryStr = """Select * From AlgorithmConfiguration WHERE AlgorithmName = %s"""
            QueryData = (
                AlgorithmConfigurationNameStr,
            )
        else:
            QueryStr = """Select * From AlgorithmConfiguration WHERE Id = %s"""
            QueryData = (
                AlgorithmConfigurationId,
            )


        AlgorithmConfigurationObj = self.templateDatabaseRetriever(QueryStr, QueryData, "getAlgorithmConfigurationObj")
        if AlgorithmConfigurationObj is None:
            return
        return AlgorithmConfigurationObj[0]
    # endregion

    # region Functions used to retrieve information from the database
    def getWebhookAlgorithmConfigurationObj(self, ApiSecretStr, ApiKeyStr):
        # print("get Webhook Obj")
        QueryStr = """Select * From AlgorithmConfiguration Where ApiSecret = %s And ApiKey = %s"""

        QueryData = (
            ApiSecretStr,
            ApiKeyStr,
        )

        return self.templateDatabaseRetriever(QueryStr, QueryData, "getAlgorithmConfigurationObj")

    # endregion