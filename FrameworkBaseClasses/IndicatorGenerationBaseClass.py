from FrameworkBaseClasses.ProcessBaseClass import ProcessBaseClass
from assets import constants as Constant
from assets import ProjectFunctions

import ccxt
import time
from datetime import datetime
import mysql.connector


class IndicatorGenerationBaseClass(ProcessBaseClass):
    def __init__(self):
        # print("Indicator Generation Base Class Constructor")
        self.ProcessName = "Indicator Generation Process"
        super().__init__()

    # region Functions used to retrieve information from the exchange

    # This function will retrieve the latest (LimitInt) candles
    def get1mCandles(self, SinceInt: int):
        # print("get 1m Candles")
        if self.ExchangeConnectionObj.has['fetchOHLCV']:
            time.sleep(self.ExchangeConnectionObj.rateLimit / 1000)
            for iterator in range(0, Constant.RETRY_LIMIT):
                try:
                    CandlestickDataArr = self.ExchangeConnectionObj.fetch_ohlcv(
                        self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                        "1m",
                        since=SinceInt
                    )
                    return CandlestickDataArr

                except ccxt.NetworkError as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT-1:
                        continue
                    self.createExchangeInteractionLog(
                        self.ProcessName,
                        datetime.utcnow(),
                        "fetch_ohlcv("
                        + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                        + "," + "1m,since="
                        + str(SinceInt) + ")",
                        "NetworkError: " + str(ErrorMessage)
                    )
                except ccxt.ExchangeError as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT-1:
                        continue
                    self.createExchangeInteractionLog(
                        self.ProcessName,
                        datetime.utcnow(),
                        "fetch_ohlcv("
                        + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                        + "," + "1m,since="
                        + str(SinceInt) + ")",
                        "ExchangeError: " + str(ErrorMessage)
                    )
                except Exception as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT-1:
                        continue
                    self.createExchangeInteractionLog(
                        self.ProcessName,
                        datetime.utcnow(),
                        "fetch_ohlcv("
                        + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                        + "," + "1m,since="
                        + str(SinceInt) + ")",
                        "OtherError: " + str(ErrorMessage)
                    )

        else:
            return False
    # endregion

    # region Functions used to update indicators for an algorithm
    def updateCandleArr(self):
        CandleDurationInt = \
            int(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_CANDLE_DURATION_INDEX])

        OutstandingCandlestickArr = self.get1mCandles(
            self.CandleArr['FiveMinuteCandles'][len(self.CandleArr['FiveMinuteCandles']) - 1]['time_stamp'] + 1000)
        if len(OutstandingCandlestickArr) < 5:
            return

        for iterator in range(0, len(OutstandingCandlestickArr), CandleDurationInt):
            if len(OutstandingCandlestickArr) < iterator + CandleDurationInt:
                break
            CandlestickSliceArr = OutstandingCandlestickArr[iterator: iterator + CandleDurationInt]
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

            self.CandleArr['FiveMinuteCandles'].append({
                'mid': (OutstandingCandlestickArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX] + OutstandingCandlestickArr[iterator + CandleDurationInt - 1][Constant.CANDLE_CLOSING_PRICE_INDEX]) / 2,
                'open': OutstandingCandlestickArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX],
                'close': OutstandingCandlestickArr[iterator + CandleDurationInt - 1][Constant.CANDLE_CLOSING_PRICE_INDEX],
                'low': MinPriceFloat,
                'high': MaxPriceFloat,
                'time_stamp': OutstandingCandlestickArr[iterator + CandleDurationInt - 1][
                    Constant.CANDLE_TIMESTAMP_INDEX]
            })

            self.CandleArr['FiveMinuteCandles'].pop(0)

    def updateBollingerBandIndicator(self):
        BollingerBandObj = ProjectFunctions.getBollingerBands(self.CandleArr['FiveMinuteCandles'], self.AlgorithmConfigurationObj)
        if 'upper' in BollingerBandObj and 'lower' in BollingerBandObj and \
                ProjectFunctions.checkIfNumber(BollingerBandObj['upper']) and \
                ProjectFunctions.checkIfNumber(BollingerBandObj['lower']):
            self.IndicatorsObj['BB']['upper'] = BollingerBandObj['upper']
            self.IndicatorsObj['BB']['lower'] = BollingerBandObj['lower']
            self.createIndicatorUpdateLog(self.ProcessName,  datetime.utcnow(), 'Bollinger Band', BollingerBandObj, 'True')
        else:
            self.createIndicatorUpdateLog(self.ProcessName,  datetime.utcnow(), 'Bollinger Band', {}, 'False')

    def updateRsiBandIndicator(self):
        RsiBandObj = ProjectFunctions.getRsiBands(self.CandleArr['FiveMinuteCandles'], self.AlgorithmConfigurationObj)
        if 'upper' in RsiBandObj and 'lower' in RsiBandObj and \
                ProjectFunctions.checkIfNumber(RsiBandObj['upper']) and \
                ProjectFunctions.checkIfNumber(RsiBandObj['lower']):
            self.IndicatorsObj['RSI']['upper'] = RsiBandObj['upper']
            self.IndicatorsObj['RSI']['lower'] = RsiBandObj['lower']
            self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'RSI Band', RsiBandObj, 'True')
        else:
            self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'RSI Band', {}, 'False')

    def updateSmaIndicator(self):
        if ProjectFunctions.checkIfNumber(self.IndicatorsObj['SMA']['value']):
            SimpleMovingAverageObj = ProjectFunctions.getSimpleMovingAverage(self.CandleArr['FiveMinuteCandles'])
            self.IndicatorsObj['SMA']['value'] = SimpleMovingAverageObj['value']
            self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'SMA',
                                          {'SMA': self.IndicatorsObj['SMA']}, 'True')
        else:
            self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'SMA',
                                          {}, 'False')

    def updateEmaIndicator(self):
        FrameCountInt = int(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_FRAME_COUNT_INDEX])
        EmaSmoothingFactorFloat = float(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EMA_SMOOTHING_FACTOR_INDEX])
        if self.IndicatorsObj['EMA']['value'] is None:
            PreviousExponentialMovingAverageFloat = ProjectFunctions.getSimpleMovingAverage(self.CandleArr['FiveMinuteCandles'][0:len(self.CandleArr['FiveMinuteCandles'])-2])['value']
        else:
            PreviousExponentialMovingAverageFloat = self.IndicatorsObj['EMA']['value']

        UpdatedEmaObj = ProjectFunctions.getExponentialMovingAverage(self.CandleArr['FiveMinuteCandles'], FrameCountInt, PreviousExponentialMovingAverageFloat, EmaSmoothingFactorFloat)
        if ProjectFunctions.checkIfNumber(UpdatedEmaObj['value']):
            self.IndicatorsObj['EMA']['value'] = UpdatedEmaObj['value']
            self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'EMA',
                                          {'EMA': self.IndicatorsObj['EMA']}, 'True')
        else:
            self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'EMA',
                                          {}, 'False')

    def updateEmaRetestIndicator(self):
        if not ProjectFunctions.checkIfNumber(self.IndicatorsObj['EMA']['value']):
            return

        LatestEmaValue = self.IndicatorsObj['EMA']['value']
        LatestCandleObj = self.CandleArr['FiveMinuteCandles'][len(self.CandleArr['FiveMinuteCandles'])-1]
        if LatestCandleObj['open'] > LatestEmaValue and LatestCandleObj['close'] > LatestEmaValue:
            CurrentMarketPlacementStr = 'above'
        elif LatestCandleObj['open'] < LatestEmaValue and LatestCandleObj['close'] < LatestEmaValue:
            CurrentMarketPlacementStr = 'below'
        else:
            CurrentMarketPlacementStr = 'all over'

        if self.IndicatorsObj['EMA_RETEST']['prev_EMA'] is None:
            if CurrentMarketPlacementStr != 'all over':
                self.IndicatorsObj['EMA_RETEST']['retest_candle_count'] = 1
            else:
                self.IndicatorsObj['EMA_RETEST']['retest_candle_count'] = 0
        else:
            if CurrentMarketPlacementStr != 'all over' and CurrentMarketPlacementStr == self.IndicatorsObj['EMA_RETEST']['placement']:
                self.IndicatorsObj['EMA_RETEST']['retest_candle_count'] += 1
            elif CurrentMarketPlacementStr != 'all over':
                self.IndicatorsObj['EMA_RETEST']['retest_candle_count'] = 1
            else:
                self.IndicatorsObj['EMA_RETEST']['retest_candle_count'] = 0

        self.IndicatorsObj['EMA_RETEST']['prev_EMA'] = LatestEmaValue
        self.IndicatorsObj['EMA_RETEST']['prev_candle'] = LatestCandleObj
        self.IndicatorsObj['EMA_RETEST']['placement'] = CurrentMarketPlacementStr

        if ProjectFunctions.checkIfNumber(self.IndicatorsObj['EMA_RETEST']['retest_candle_count']):
            self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'EMA_RETEST',
                                          {'EMA_RETEST': self.IndicatorsObj['EMA_RETEST']}, 'True')
        else:
            self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'EMA_RETEST',
                                          {}, 'False')

    def updateClosingOrderCountIndicator(self):
        if self.CurrentSystemVariables['CurrentAccountPositionSize'] != 0 and self.IndicatorsObj['EMA_RETEST']['retest_candle_count'] >= self.IndicatorsObj['COC']['OrderCount']:
            self.IndicatorsObj['COC']['OrderCount'] += 1
            self.IndicatorsObj['COC']['RetestCount'] = self.IndicatorsObj['EMA_RETEST']['retest_candle_count']
        elif self.CurrentSystemVariables['CurrentAccountPositionSize'] != 0 and self.IndicatorsObj['EMA_RETEST']['retest_candle_count'] < self.IndicatorsObj['COC']['OrderCount']:
            self.IndicatorsObj['COC']['OrderCount'] = self.IndicatorsObj['EMA_RETEST']['retest_candle_count']
            self.IndicatorsObj['COC']['RetestCount'] = self.IndicatorsObj['EMA_RETEST']['retest_candle_count']
        elif self.CurrentSystemVariables['CurrentAccountPositionSize'] == 0 and self.IndicatorsObj['EMA_RETEST']['retest_candle_count'] < self.IndicatorsObj['COC']['RetestCount']:
            self.IndicatorsObj['COC']['OrderCount'] = 0
            self.IndicatorsObj['COC']['RetestCount'] = self.IndicatorsObj['EMA_RETEST']['retest_candle_count']

        self.createIndicatorUpdateLog(self.ProcessName, datetime.utcnow(), 'COC',
                                      self.IndicatorsObj['COC'], 'True')
    # endregion
