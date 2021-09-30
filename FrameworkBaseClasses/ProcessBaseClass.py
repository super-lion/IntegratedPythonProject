from assets import constants as Constant
from assets import ProjectFunctions

import sys as SystemObj
import mysql.connector
from datetime import datetime, timezone
import time


class ProcessBaseClass:
    DatabaseConnectionDetails = {
        'ServerName': '',
        'DatabaseName': '',
        'UserName': '',
        'Password': ''
    }
    ExchangeConnectionDetails = {
        'ExchangeName': '',
        'ApiKey': '',
        'ApiSecret': ''
    }
    ExchangeConnectionObj = None
    AlgorithmConfigurationObj = None
    ProcessName = ""
    ObjectTypeValidationArr = []

    # region Variables provided by Manager class
    IndicatorsObj = None
    CurrentSystemVariables = None
    CandleArr = []
    # endregion

    def initiateExecution(self):
        # print("Initiating process execution flow")
        pass

    # region Functions used to validate and store variables provided by the manager class
    def setExchangeConnectionObj(self, Object):
        self.ExchangeConnectionObj = Object

    def setAlgorithmConfigurationObj(self, Object):
        self.AlgorithmConfigurationObj = Object

    def setExchangeConnectionDetailsObj(self, Object):
        if self.validateObject(Object, 'ExchangeDetails'):
            self.ExchangeConnectionDetails = Object
        else:
            print("Process Base Class for " + self.ProcessName + ": please provide valid Exchange Details")
            SystemObj.exit()

    def setDatabaseConnectionDetailsObj(self, Object):
        if self.validateObject(Object, 'DatabaseDetails'):
            self.DatabaseConnectionDetails = Object
        else:
            print("Process Base Class for " + self.ProcessName + ": please provide valid Database Details")
            SystemObj.exit()

    def setIndicators(self, Object):
        if self.validateObject(Object, 'Indicators'):
            self.IndicatorsObj = Object
        else:
            print("Process Base Class for " + self.ProcessName + ": please provide valid Indicator Object")
            SystemObj.exit()

    def setSystemVariables(self, Object):
        if self.validateObject(Object, 'SystemVariables'):
            self.CurrentSystemVariables = Object
        else:
            print("Process Base Class for " + self.ProcessName + ": please provide valid System Variable Object")
            SystemObj.exit()

    def setCandleArr(self, Object):
        if self.validateObject(Object, 'CandleArr'):
            self.CandleArr = Object
        else:
            print("Process Base Class for " + self.ProcessName + ": please provide valid Candle Arr")
            SystemObj.exit()

    def validateObject(self, Object, ObjectTypeStr):
        ValidationResultBool = True
        if ObjectTypeStr in self.ObjectTypeValidationArr:
            RequiredParametersArr = self.ObjectTypeValidationArr[ObjectTypeStr]
        else:
            return False

        for key in RequiredParametersArr:
            if key not in Object:
                ValidationResultBool = False

        return ValidationResultBool
    # endregion

    # region Functions used in initializing system data
    def initializeEmaAndRetest(self, GivenEmaObj, GivenEmaRetestObj):
        # print('Initializing EMA Retest Indicator')

        CandleDurationInt = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_CANDLE_DURATION_INDEX]
        FrameCountInt = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_FRAME_COUNT_INDEX]
        EmaSmoothingFactor = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EMA_SMOOTHING_FACTOR_INDEX]

        RawCandleDataArr = self.get1mCandles(CandleDurationInt, FrameCountInt*2)
        ProcessedCandleDataArr = []
        for iterator in range(0, len(RawCandleDataArr), CandleDurationInt):
            ProcessedCandleDataArr.append({
                'mid': (RawCandleDataArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX] +
                        RawCandleDataArr[iterator + CandleDurationInt-1][Constant.CANDLE_CLOSING_PRICE_INDEX])/2,
                'open': RawCandleDataArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX],
                'close': RawCandleDataArr[iterator + CandleDurationInt-1][Constant.CANDLE_CLOSING_PRICE_INDEX],
                'time_stamp':
                    RawCandleDataArr[iterator + CandleDurationInt-1][Constant.CANDLE_TIMESTAMP_INDEX]
            })
        SmaObj = ProjectFunctions.getSimpleMovingAverage(ProcessedCandleDataArr[0:FrameCountInt-1])
        HistoricalEmaDataArr = []
        PreviousEmaFloat = None
        for index in range(0, FrameCountInt):
            if PreviousEmaFloat is None:
                PreviousEmaFloat = SmaObj['value']
            else:
                PreviousEmaFloat = HistoricalEmaDataArr[len(HistoricalEmaDataArr)-1]['value']
            HistoricalEmaDataArr.append(ProjectFunctions.getExponentialMovingAverage(ProcessedCandleDataArr[index+1: FrameCountInt+index], FrameCountInt, PreviousEmaFloat, EmaSmoothingFactor))
        EmaRetestCounterInt = 0
        LatestEmaObj = HistoricalEmaDataArr[len(HistoricalEmaDataArr)-1]
        LatestCandleObj = ProcessedCandleDataArr[len(ProcessedCandleDataArr)-1]
        if LatestCandleObj['open'] > LatestEmaObj['value'] and LatestCandleObj['close'] > LatestEmaObj['value']:
            EmaPlacementStr = 'above'
        elif LatestCandleObj['open'] < LatestEmaObj['value'] and LatestCandleObj['close'] < LatestEmaObj['value']:
            EmaPlacementStr = 'below'
        else:
            EmaPlacementStr = 'all over'

        for index in range(FrameCountInt, 0, -1):
            if ProcessedCandleDataArr[FrameCountInt+index-1]['open'] > HistoricalEmaDataArr[index-1]['value'] and ProcessedCandleDataArr[FrameCountInt+index-1]['close'] > HistoricalEmaDataArr[index-1]['value']:
                TempPlacementStr = 'above'
            elif ProcessedCandleDataArr[FrameCountInt+index-1]['open'] < HistoricalEmaDataArr[index-1]['value'] and ProcessedCandleDataArr[FrameCountInt+index-1]['close'] < HistoricalEmaDataArr[index-1]['value']:
                TempPlacementStr = 'below'
            else:
                TempPlacementStr = 'all over'

            if TempPlacementStr == EmaPlacementStr and EmaPlacementStr != 'all over':
                EmaRetestCounterInt += 1
            else:
                break
        GivenEmaObj['value'] = LatestEmaObj['value']
        GivenEmaRetestObj['prev_EMA'] = LatestEmaObj['value']
        GivenEmaRetestObj['prev_candle'] = LatestCandleObj
        GivenEmaRetestObj['retest_candle_count'] = EmaRetestCounterInt
        GivenEmaRetestObj['placement'] = EmaPlacementStr

    # endregion

    # region Base function used to retrieve information from the database
    def templateDatabaseRetriever(self, QueryStr, QueryData, FunctionNameStr=" "):
        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                ConnectionObj = mysql.connector.connect(host=self.DatabaseConnectionDetails['ServerName'],
                                                        database=self.DatabaseConnectionDetails['DatabaseName'],
                                                        user=self.DatabaseConnectionDetails['UserName'],
                                                        password=self.DatabaseConnectionDetails['Password'])

                CursorObj = ConnectionObj.cursor(buffered=True)
                CursorObj.execute(QueryStr, QueryData)
                RetrievedDataObj = CursorObj.fetchall()
                if ConnectionObj.is_connected():
                    CursorObj.close()
                    ConnectionObj.close()
                else:
                    print("Failed to close MySQL connection")

                return RetrievedDataObj

            except mysql.connector.Error as error:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                print(
                    self.ProcessName + " in " + FunctionNameStr + " failed to retrieve information from MySQL database {}".format(error))
                print(QueryStr)
                print(QueryData)
                print(datetime.utcnow())
    # endregion

    # region Base function used to insert information into the database
    def templateDatabaseLogger(self, QueryStr, QueryData, FunctionNameStr=" "):
        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                ConnectionObj = mysql.connector.connect(host=self.DatabaseConnectionDetails['ServerName'],
                                                        database=self.DatabaseConnectionDetails['DatabaseName'],
                                                        user=self.DatabaseConnectionDetails['UserName'],
                                                        password=self.DatabaseConnectionDetails['Password'])

                CursorObj = ConnectionObj.cursor()
                CursorObj.execute(QueryStr, QueryData)
                ConnectionObj.commit()

                if ConnectionObj.is_connected():
                    CursorObj.close()
                    ConnectionObj.close()
                else:
                    print("Failed to close MySQL connection")
                break

            except mysql.connector.Error as error:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                print(self.ProcessName + " in " + FunctionNameStr + " failed to insert into MySQL table {}".format(error))
                print(QueryStr)
                print(QueryData)
                print(datetime.utcnow())
    # endregion

    # region Functions used to log process successes and failures as system executes
    def createProcessExecutionLog(self, ProcessNameStr, EntryDateTimeObj, MessageStr):
        # print("create Process Execution Log")
        SelectedAlgorithmId = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ID_INDEX]

        QueryStr = """INSERT INTO ProcessExecutionLog (ProcessName, EntryTime, Message, AlgorithmConfiguration) 
                               VALUES 
                               (%s, %s, %s, %s)"""

        QueryData = (
            ProcessNameStr,
            EntryDateTimeObj,
            MessageStr,
            SelectedAlgorithmId
        )

        self.templateDatabaseLogger(QueryStr, QueryData, "createProcessExecutionLog")

    def createExchangeInteractionLog(self, ProcessNameStr, EntryDateTimeObj, ExchangeFunctionStr, MessageStr):
        SelectedAlgorithmId = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ID_INDEX]

        QueryStr = """INSERT INTO ExchangeInteractionFailureLog (ProcessName, EntryTime, ExchangeFunction, ErrorMessage, AlgorithmConfiguration)
                                       VALUES
                                       (%s, %s, %s, %s, %s)"""
        QueryData = (
            ProcessNameStr,
            EntryDateTimeObj,
            ExchangeFunctionStr,
            MessageStr,
            SelectedAlgorithmId
        )
        self.templateDatabaseLogger(QueryStr, QueryData, "createExchangeInteractionLog")

    def createIndicatorUpdateLog(self, ProcessNameStr, EntryDateTimeObj, IndicatorNameStr, IndicatorDataObj,
                                 SuccessStr):
        SelectedAlgorithmId = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ID_INDEX]

        QueryStr = """INSERT INTO IndicatorGenerationLog (EntryTime, IndicatorData, Success, ProcessName, IndicatorName, AlgorithmConfiguration)
                                       VALUES
                                       (%s, %s, %s, %s, %s, %s)"""

        QueryData = (
            EntryDateTimeObj,
            str(IndicatorDataObj),
            SuccessStr,
            ProcessNameStr,
            IndicatorNameStr,
            SelectedAlgorithmId
        )
        self.templateDatabaseLogger(QueryStr, QueryData, "createIndicatorUpdateLog")

    def createPriceLogEntry(self, EntryDateTimeObj, CurrencyPrice):
        # print("create Process Execution Log")
        QueryStr = """INSERT INTO PriceLog (ExchangeName, CurrencySymbol, EntryTime, CurrencyPrice) 
                                      VALUES 
                                      (%s, %s, %s, %s)"""

        QueryData = (
            self.ExchangeConnectionDetails['ExchangeName'],
            self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
            EntryDateTimeObj,
            CurrencyPrice
        )

        self.templateDatabaseLogger(QueryStr, QueryData, "createPriceLogEntry")

    def createOrderLog(self, EntryDateTimeObj, OrderPriceFloat, OrderActionStr, OrderDirectionStr, OrderQuantityInt, TradingState):
        SelectedAlgorithmId = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ID_INDEX]

        QueryStr = """INSERT INTO OrderLog (EntryTime, OrderPrice, OrderAction, OrderDirection, OrderQuantity, AlgorithmConfiguration, TradingState)
                                               VALUES
                                               (%s, %s, %s, %s, %s, %s, %s)"""

        QueryData = (
            EntryDateTimeObj,
            OrderPriceFloat,
            OrderActionStr,
            OrderDirectionStr,
            str(OrderQuantityInt),
            SelectedAlgorithmId,
            TradingState,
        )
        self.templateDatabaseLogger(QueryStr, QueryData, "createOrderLog")

    def createAlgorithmSnapshot(self, PortfolioValueFloat, PositionSizeFloat, EntryTimeObj):
        SelectedAlgorithmId = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ID_INDEX]
        QueryStr = """INSERT INTO AlgorithmSnapshot (PortfolioValue, PositionSize, EntryTime, AlgorithmConfiguration)
                                                       VALUES
                                                       (%s, %s, %s, %s)"""
        QueryData = (
            PortfolioValueFloat,
            PositionSizeFloat,
            EntryTimeObj,
            SelectedAlgorithmId,
        )
        self.templateDatabaseLogger(QueryStr, QueryData, "createAlgorithmSnapshot")
        pass

    # endregion

    # region Functions used to retrieve information from the exchange
    # This function will retrieve the latest (LimitInt) candles
    def get1mCandles(self, CandleDurationInt: int, FrameCountInt: int):
        # print("get 1m Candles")
        if self.ExchangeConnectionObj.has['fetchOHLCV']:
            time.sleep(self.ExchangeConnectionObj.rateLimit / 1000)
            LimitInt = CandleDurationInt * FrameCountInt
            SinceTimeInt = round((time.time() * 1000) - (1000 * LimitInt * 60))
            ExtraTimeRoundOffInt = SinceTimeInt % (CandleDurationInt * 60 * 1000)
            SinceTimeInt = SinceTimeInt - ExtraTimeRoundOffInt

            for iterator in range(0, Constant.RETRY_LIMIT):
                try:
                    CandlestickDataArr = self.ExchangeConnectionObj.fetch_ohlcv(
                        self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                        "1m",
                        since=SinceTimeInt,
                        limit=LimitInt
                    )
                    return CandlestickDataArr
                except Exception as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT - 1:
                        continue
                    self.createExchangeInteractionLog(
                        self.ProcessName,
                        datetime.utcnow(),
                        "fetch_ohlcv("
                        + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                        + "," + "1m,since=("
                        + str((time.time() * 1000) - (1000 * LimitInt * 60)) + "),limit=" + str(LimitInt) + ")",
                        ErrorMessage
                    )

        else:
            return False

    def getMyTrades(self, TimeSpan):
        # print('get my trades')
        if self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EXCHANGE_NAME_INDEX] == Constant.BINANCE_EXCHANGE_ID:
            time.sleep(self.ExchangeConnectionObj.rateLimit / 1000)
            SinceTimeInt = round((time.time() * 1000) - (1000 * TimeSpan * 60))
            for iterator in range(0, Constant.RETRY_LIMIT):
                try:
                    TradeDataArr = self.ExchangeConnectionObj.sapi_get_margin_mytrades({
                        'symbol': 'BTCUSDT',
                        'startTime': SinceTimeInt
                    })
                    if TradeDataArr is None:
                        return []
                    return TradeDataArr
                except Exception as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT-1:
                        continue
                    self.createExchangeInteractionLog(
                        self.ProcessName,
                        datetime.utcnow(),
                        "sapi_get_margin_mytrades(symbol="
                        + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                        + "," + "since=("
                        + str(SinceTimeInt) + "))",
                        ErrorMessage
                    )
                    return []
        else:
            return []
    # endregion
