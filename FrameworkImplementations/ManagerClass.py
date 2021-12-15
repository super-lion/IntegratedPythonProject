from FrameworkBaseClasses.ManagerBaseClass import ManagerBaseClass
from FrameworkImplementations.IndicatorGenerationClass import IndicatorGenerationClass
from FrameworkImplementations.RiskManagementClass import RiskManagementClass
from FrameworkImplementations.TraderClass import TraderClass
from assets import constants as Constant
from assets import ProjectFunctions

from datetime import datetime, timedelta
import sys as SystemObj
import time
from threading import Timer


class ManagerClass(ManagerBaseClass):
    FiveMinCandleArr = []
    CurrentSimpleMovingAverageFloat = None
    CurrentExponentialMovingAverageRetestObj = {
        'prev_EMA': None,
        'prev_candle': None,
        'retest_candle_count': None,
        'placement': None
    }
    CurrentExponentialMovingAverageObj = {
        'value': None
    }
    BollingerBandObj = {
        'upper': None,
        'lower': None
    }
    RsiBandObj = {
        'upper': None,
        'lower': None
    }
    IndicatorTimeStampObj = {
        'datetime': None
    }
    CloseOrderCountObj = {
        'OrderCount': None,
        'RetestCount': None
    }

    def __init__(self):
        # print("Manager Class Constructor")
        super().__init__()
        if self.SystemVariablesObj['SystemState'] == 'Passive':
            return

        self.initializeProcessObjects()

        if not self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_WEBHOOK]:
            self.initiateStartingTimer()
        self.startProcessThreading()

    def initializeProcessObjects(self):
        # print("Initializing Process Objects")
        AlgorithmNameStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]

        self.IndicatorGenerationObj = IndicatorGenerationClass()
        self.IndicatorGenerationObj.setAlgorithmConfigurationObj(self.AlgorithmConfigurationObj)
        self.IndicatorGenerationObj.setExchangeConnectionObj(self.ExchangeConnectionObj)
        self.IndicatorGenerationObj.setDatabaseConnectionDetailsObj(self.DatabaseConnectionDetails)
        self.IndicatorGenerationObj.setSystemVariables(self.SystemVariablesObj)
        self.IndicatorGenerationObj.setIndicators({
            'BB': self.BollingerBandObj,
            'RSI': self.RsiBandObj,
            'SMA': self.CurrentSimpleMovingAverageFloat,
            'EMA': self.CurrentExponentialMovingAverageObj,
            'EMA_RETEST': self.CurrentExponentialMovingAverageRetestObj,
            'COC': self.CloseOrderCountObj,
            'TimeStamp': self.IndicatorTimeStampObj
        })
        self.IndicatorGenerationObj.setCandleArr({
            'FiveMinuteCandles': self.FiveMinCandleArr
        })

        self.RiskManagementObj = RiskManagementClass()
        self.RiskManagementObj.setAlgorithmConfigurationObj(self.AlgorithmConfigurationObj)
        self.RiskManagementObj.setExchangeConnectionObj(self.ExchangeConnectionObj)
        self.RiskManagementObj.setDatabaseConnectionDetailsObj(self.DatabaseConnectionDetails)
        self.RiskManagementObj.setSystemVariables(self.SystemVariablesObj)
        self.RiskManagementObj.setExchangeConnectionDetailsObj(self.ExchangeConnectionDetails)
        self.RiskManagementObj.setIndicators({
            'BB': self.BollingerBandObj,
            'RSI': self.RsiBandObj,
            'SMA': self.CurrentSimpleMovingAverageFloat,
            'EMA': self.CurrentExponentialMovingAverageObj,
            'EMA_RETEST': self.CurrentExponentialMovingAverageRetestObj,
            'COC': self.CloseOrderCountObj,
            'TimeStamp': self.IndicatorTimeStampObj
        })

        self.TraderObj = TraderClass()
        self.TraderObj.setExchangeConnectionObj(self.ExchangeConnectionObj)
        self.TraderObj.setAlgorithmConfigurationObj(self.AlgorithmConfigurationObj)
        self.TraderObj.setExchangeConnectionDetailsObj(self.ExchangeConnectionDetails)
        self.TraderObj.setIndicators({
            'BB': self.BollingerBandObj,
            'RSI': self.RsiBandObj,
            'SMA': self.CurrentSimpleMovingAverageFloat,
            'EMA': self.CurrentExponentialMovingAverageObj,
            'EMA_RETEST': self.CurrentExponentialMovingAverageRetestObj,
            'COC': self.CloseOrderCountObj,
            'TimeStamp': self.IndicatorTimeStampObj
        })
        self.TraderObj.setSystemVariables(self.SystemVariablesObj)
        self.TraderObj.setDatabaseConnectionDetailsObj(self.DatabaseConnectionDetails)

        if AlgorithmNameStr == Constant.PRICE_DATA_GENERATION_BASE_VERSION:
            self.ThreadInstantiationArr = [
                {'ProcessObj': self.TraderObj, 'IntervalInt': 1},
                {'ProcessObj': self, 'IntervalInt': 1}
            ]
        else:

            if not self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_WEBHOOK]:
                IndicatorGenerationIntervalInt = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_CANDLE_DURATION_INDEX] * 60
                self.ThreadInstantiationArr = [
                    {'ProcessObj': self.IndicatorGenerationObj, 'IntervalInt': IndicatorGenerationIntervalInt},
                    {'ProcessObj': self.RiskManagementObj, 'IntervalInt': 15},
                    {'ProcessObj': self.TraderObj, 'IntervalInt': 10},
                    {'ProcessObj': self, 'IntervalInt': 6}
                ]
            else:
                self.ThreadInstantiationArr = [
                    {'ProcessObj': self.RiskManagementObj, 'IntervalInt': 15},
                    {'ProcessObj': self, 'IntervalInt': 6}
                ]

    def initializeSystemData(self):
        # print('Initializing system data')
        AlgorithmNameStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]
        if AlgorithmNameStr == Constant.PRICE_DATA_GENERATION_BASE_VERSION:
            return
        # region Indicator Initialization
        CandleDurationInt = \
            int(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_CANDLE_DURATION_INDEX])
        FrameCountInt = \
            int(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_FRAME_COUNT_INDEX])

        BollingerBandUsedBool = False
        if self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_BB_STANDARD_DEVIATION_INDEX] is not None:
            BollingerBandUsedBool = True

        RsiUsedBool = False
        if self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_RSI_LOWER_INTENSITY_INDEX] is not None and self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_RSI_UPPER_INTENSITY_INDEX] is not None:
            RsiUsedBool = True

        EmaUsedBool = False
        if self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EMA_SMOOTHING_FACTOR_INDEX] is not None:
            EmaUsedBool = True

        CandlestickDataArr = self.get1mCandles(CandleDurationInt, FrameCountInt)

        for iterator in range(0, len(CandlestickDataArr), CandleDurationInt):
            CandlestickSliceArr = CandlestickDataArr[iterator: iterator + CandleDurationInt]
            MinPriceFloat = None
            MaxPriceFloat = None
            for CandlestickObj in CandlestickSliceArr:
                if MinPriceFloat is None or MaxPriceFloat is None:
                    MinPriceFloat = CandlestickObj[Constant.CANDLE_LOWEST_PRICE_INDEX]
                    MaxPriceFloat = CandlestickObj[Constant.CANDLE_HIGHEST_PRICE_INDEX]
                    continue
                if CandlestickObj[Constant.CANDLE_LOWEST_PRICE_INDEX] < MinPriceFloat:
                    MinPriceFloat = CandlestickObj[Constant.CANDLE_LOWEST_PRICE_INDEX]

                if CandlestickObj[Constant.CANDLE_HIGHEST_PRICE_INDEX] > MaxPriceFloat:
                    MaxPriceFloat = CandlestickObj[Constant.CANDLE_HIGHEST_PRICE_INDEX]

            self.FiveMinCandleArr.append({
                'mid': (CandlestickDataArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX] +
                        CandlestickDataArr[iterator + CandleDurationInt-1][Constant.CANDLE_CLOSING_PRICE_INDEX])/2,
                'open': CandlestickDataArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX],
                'close': CandlestickDataArr[iterator + CandleDurationInt-1][Constant.CANDLE_CLOSING_PRICE_INDEX],
                'low': MinPriceFloat,
                'high': MaxPriceFloat,
                'time_stamp':
                    CandlestickDataArr[iterator + CandleDurationInt-1][Constant.CANDLE_TIMESTAMP_INDEX]
            })

        if BollingerBandUsedBool:
            BollingerBandObj = ProjectFunctions.getBollingerBands(self.FiveMinCandleArr, self.AlgorithmConfigurationObj)
            if 'upper' in BollingerBandObj and 'lower' in BollingerBandObj and\
                    ProjectFunctions.checkIfNumber(BollingerBandObj['upper']) and\
                    ProjectFunctions.checkIfNumber(BollingerBandObj['lower']):
                self.createIndicatorUpdateLog(self.ProcessName,  datetime.utcnow(), 'Bollinger Band', BollingerBandObj, 'True')
                self.BollingerBandObj = BollingerBandObj
            else:
                self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'Bollinger Band', {}, 'False')

        if RsiUsedBool:
            RsiBandObj = ProjectFunctions.getRsiBands(self.FiveMinCandleArr, self.AlgorithmConfigurationObj)
            if 'upper' in RsiBandObj and 'lower' in RsiBandObj and\
                    ProjectFunctions.checkIfNumber(RsiBandObj['upper']) and\
                    ProjectFunctions.checkIfNumber(RsiBandObj['lower']):
                self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'RSI Band', RsiBandObj, 'True')
                self.RsiBandObj = RsiBandObj
            else:
                self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'RSI Band', {}, 'False')

        self.CloseOrderCountObj = {
            'OrderCount': 0,
            'RetestCount': 0
        }
        self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'COC', self.CloseOrderCountObj, 'True')

        self.CurrentSimpleMovingAverageFloat = ProjectFunctions.getSimpleMovingAverage(self.FiveMinCandleArr)
        self.IndicatorTimeStampObj = {'datetime': datetime.utcnow()}

        if ProjectFunctions.checkIfNumber(self.CurrentSimpleMovingAverageFloat['value']):
            self.createIndicatorUpdateLog(self.ProcessName, self.IndicatorTimeStampObj['datetime'], 'SMA',
                                          {'SMA': self.CurrentSimpleMovingAverageFloat['value']}, 'True')
        else:
            self.createIndicatorUpdateLog(self.ProcessName, self.IndicatorTimeStampObj['datetime'], 'SMA',
                                          {}, 'False')

        if EmaUsedBool:
            self.initializeEmaAndRetest(self.CurrentExponentialMovingAverageObj, self.CurrentExponentialMovingAverageRetestObj)
            if ProjectFunctions.checkIfNumber(self.CurrentExponentialMovingAverageObj['value']):
                self.createIndicatorUpdateLog(self.ProcessName, self.IndicatorTimeStampObj['datetime'], 'EMA',
                                              {'EMA': self.CurrentExponentialMovingAverageObj['value']}, 'True')
            else:
                self.createIndicatorUpdateLog(self.ProcessName, self.IndicatorTimeStampObj['datetime'], 'EMA',
                                              {}, 'False')
            if ProjectFunctions.checkIfNumber(self.CurrentExponentialMovingAverageRetestObj['retest_candle_count']):
                self.createIndicatorUpdateLog(self.ProcessName, self.IndicatorTimeStampObj['datetime'], 'EMA_RETEST',
                                              {'EMA_RETEST': self.CurrentExponentialMovingAverageRetestObj}, 'True')
            else:
                self.createIndicatorUpdateLog(self.ProcessName, self.IndicatorTimeStampObj['datetime'], 'EMA_RETEST',
                                              {}, 'False')
        # endregion
        # region State Variable Initialization
        self.getCurrentPrice()
        self.getCurrentBalance()
        self.getCurrentPosition()
        # endregion

    def initiateExecution(self):
        # this will be changed to a function that logs the one second price to the database
        self.getCurrentPrice()
        self.getCurrentBalance()
        self.getCurrentPosition()
        self.createAlgorithmSnapshot(
            self.SystemVariablesObj['CurrentPortfolioValue'],
            self.SystemVariablesObj['CurrentAccountPositionSize'],
            datetime.utcnow(),
        )

    def initiateStartingTimer(self):
        # region Making sure system starts at the beginning of 5 minutes
        RunStartTimerSelectionStr = input("Would you like to run the timer?(Y/N):\n" + "Input: ")

        if RunStartTimerSelectionStr != 'Y':
            return

        WhenToStartInt = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_CANDLE_DURATION_INDEX]

        if WhenToStartInt is None:
            return

        now = datetime.utcnow()
        next_run = now.replace(minute=int(now.minute / WhenToStartInt) * WhenToStartInt, second=0, microsecond=0) + timedelta(minutes=WhenToStartInt)
        sleep_time = (next_run - now).total_seconds()
        time.sleep(sleep_time)
        # endregion

    def updateSystemVariablesForWebhook(self):
        self.getCurrentPrice()
        self.getCurrentBalance()
        self.getCurrentPosition()

    def initializeTraderObjForWebhook(self):
        TraderObj = TraderClass()
        TraderObj.setSystemVariables(self.SystemVariablesObj)
        TraderObj.setExchangeConnectionObj(self.ExchangeConnectionObj)
        TraderObj.setAlgorithmConfigurationObj(self.AlgorithmConfigurationObj)
        TraderObj.setExchangeConnectionDetailsObj(self.ExchangeConnectionDetails)
        TraderObj.setDatabaseConnectionDetailsObj(self.DatabaseConnectionDetails)
        return TraderObj

    def verifyPayload(self, PayloadObj):
        VerificationArr = ['InstanceIdentifier', 'TradeAction', 'TradeDirection', 'TradeType', 'Price', 'SideEffect']

        for RequiredAttribute in VerificationArr:
            try:
                if PayloadObj[RequiredAttribute] is None:
                    print('Payload failed verification')
                    return False
            except Exception as ErrorMessage:
                print('Payload failed verification: ' + str(ErrorMessage))
                return False
        return True

    def testTradeTiming(self, PayloadObj, TraderObj):
        if PayloadObj['TradeAction'] == 'close' and self.SystemVariablesObj['CurrentAccountPositionSize'] == 0:
            print('Received order request in incorrect order! No action performed.')
            self.createProcessExecutionLog(self.ProcessName, datetime.utcnow(), 'Process Failed: received order request in incorrect order! No action performed.')
            return False
        elif PayloadObj['TradeAction'] == 'open' and self.SystemVariablesObj['CurrentAccountPositionSize'] != 0:
            print('Received order request in incorrect order! Closed position.')
            self.createProcessExecutionLog(self.ProcessName, datetime.utcnow(), 'Process Failed: received order request in incorrect order! Closed position.')
            if self.SystemVariablesObj['CurrentAccountPositionSize'] > 0:
                TraderObj.placeMarketOrder('sell')
            else:
                TraderObj.placeMarketOrder('buy')
        return True

    def processPlaceTradeApiRequest(self, PayloadObj):
        if not self.verifyPayload(PayloadObj) or not self.authenticateWebhookRequest(PayloadObj):
            print('Api call authentication failed')
            return

        self.updateSystemVariablesForWebhook()
        # print('System Variables Updated')
        TraderObj = self.initializeTraderObjForWebhook()

        if not self.testTradeTiming(PayloadObj, TraderObj):
            return

        if PayloadObj['TradeType'] == 'MARKET':
            # print('If: ' + PayloadObj['TradeType'])
            TraderObj.genericPlaceMarketTrade(PayloadObj)
        else:
            # print('Else' + PayloadObj['TradeType'])
            TraderObj.genericPlaceLimitTrade(PayloadObj)

