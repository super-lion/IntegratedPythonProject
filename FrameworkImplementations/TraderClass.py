from FrameworkBaseClasses.TraderBaseClass import TraderBaseClass
from assets import constants as Constant

from datetime import datetime
import sys as SystemObj
from time import sleep


class TraderClass(TraderBaseClass):

    def __init__(self):
        # print("Trader Class Constructor")
        super().__init__()

    def initiateExecution(self):
        AlgorithmNameStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]

        if AlgorithmNameStr == Constant.EMA_ALGORITHM_V1:
            self.ema21AnalyzerAlgorithm()
        elif AlgorithmNameStr == Constant.BB_RSI_ALGORITHM_V1 or AlgorithmNameStr == Constant.BB_RSI_ALGORITHM_V2:
            self.bbRsiTradingAlgorithm()
        elif AlgorithmNameStr == Constant.BB_RSI_ALGORITHM_V3:
            self.bbRsiTradingAlgorithmImproved()
        elif AlgorithmNameStr == Constant.PRICE_DATA_GENERATION_BASE_VERSION:
            self.priceDataGeneration()
        else:
            print('Trading algorithm ' + AlgorithmNameStr + ' was not found')
            SystemObj.exit()

    def bbRsiTradingAlgorithm(self):
        self.OpenOrderCountInt = self.countOpenOrders()
        if self.OpenOrderCountInt is False:
            self.createProcessExecutionLog(self.ProcessName, datetime.utcnow(),
                                           "Process Update: Not executing trading functionality due to issues with"
                                           " getting Order Count or Position Count")
            return

        # region Handling actions based on the trading state of the algorithm
        # trading state is managed by the risk management thread
        if not self.checkTradingState():
            return
        # endregion

        # region Actual Algorithm
        if self.OpenOrderCountInt < 1:
            if self.CurrentSystemVariables['CurrentAccountPositionSize'] > 0:
                self.placeClosingOrder('sell')
                return
            if self.CurrentSystemVariables['CurrentAccountPositionSize'] < 0:
                self.placeClosingOrder('buy')
                return
            self.placeOpeningOrders()
            return
        for CurrentOrder in self.CurrentOrderArr:
            if self.CurrentSystemVariables['CurrentAccountPositionSize'] < 0:
                if CurrentOrder['side'] != 'buy' or CurrentOrder['price'] != round(self.IndicatorsObj['SMA']['value']):
                    if self.cancelAllOrders() is True:
                        self.placeClosingOrder('buy')
                    return
            elif self.CurrentSystemVariables['CurrentAccountPositionSize'] > 0:
                if CurrentOrder['side'] != 'sell' or CurrentOrder['price'] != round(self.IndicatorsObj['SMA']['value']):
                    if self.cancelAllOrders() is True:
                        self.placeClosingOrder('sell')
                    return
            elif CurrentOrder['side'] == 'sell':
                UpperLimitArr = [self.IndicatorsObj['BB']['upper'], self.IndicatorsObj['RSI']['upper']]
                if CurrentOrder['price'] != round(max(UpperLimitArr)):
                    if self.cancelAllOrders() is True:
                        self.placeOpeningOrders()
                    return
            elif CurrentOrder['side'] == 'buy':
                LowerLimitArr = [self.IndicatorsObj['BB']['lower'], self.IndicatorsObj['RSI']['lower']]
                if CurrentOrder['price'] != round(min(LowerLimitArr)):
                    if self.cancelAllOrders() is True:
                        self.placeOpeningOrders()
                    return
        # endregion

    def bbRsiTradingAlgorithmImproved(self):
        self.OpenOrderCountInt = self.countOpenOrders()
        if self.OpenOrderCountInt is False:
            self.createProcessExecutionLog(self.ProcessName, datetime.utcnow(),
                                           "Process Update: Not executing trading functionality due to issues with"
                                           " getting Order Count or Position Count")
            return

        # region Handling actions based on the trading state of the algorithm
        # trading state is managed by the risk management thread
        if not self.checkTradingState():
            return
        # endregion

        # region Actual Algorithm
        CurrentTradingStateStr = self.CurrentSystemVariables['TradingState']
        if CurrentTradingStateStr == 'Active':
            if self.OpenOrderCountInt < 1:
                if self.CurrentSystemVariables['CurrentAccountPositionSize'] > 0:
                    self.placeClosingOrder('sell')
                    return
                if self.CurrentSystemVariables['CurrentAccountPositionSize'] < 0:
                    self.placeClosingOrder('buy')
                    return
                self.placeOpeningOrders()
                return

            for CurrentOrder in self.CurrentOrderArr:
                if self.CurrentSystemVariables['CurrentAccountPositionSize'] < 0:
                    if CurrentOrder['side'] != 'buy' or CurrentOrder['price'] != round(self.IndicatorsObj['SMA']['value']):
                        if self.cancelAllOrders() is True:
                            self.placeClosingOrder('buy')
                        return
                elif self.CurrentSystemVariables['CurrentAccountPositionSize'] > 0:
                    if CurrentOrder['side'] != 'sell' or CurrentOrder['price'] != round(self.IndicatorsObj['SMA']['value']):
                        if self.cancelAllOrders() is True:
                            self.placeClosingOrder('sell')
                        return
                elif CurrentOrder['side'] == 'sell':
                    UpperLimitArr = [self.IndicatorsObj['BB']['upper'], self.IndicatorsObj['RSI']['upper']]
                    if CurrentOrder['price'] != round(max(UpperLimitArr)):
                        if self.cancelAllOrders() is True:
                            self.placeOpeningOrders()
                        return
                elif CurrentOrder['side'] == 'buy':
                    LowerLimitArr = [self.IndicatorsObj['BB']['lower'], self.IndicatorsObj['RSI']['lower']]
                    if CurrentOrder['price'] != round(min(LowerLimitArr)):
                        if self.cancelAllOrders() is True:
                            self.placeOpeningOrders()
                        return
        elif CurrentTradingStateStr == 'Reverse':

            if self.IndicatorsObj['EMA_RETEST']['placement'] == 'above' and self.CurrentSystemVariables['CurrentPrice'] < self.IndicatorsObj['SMA']['value']:
                return
            elif self.IndicatorsObj['EMA_RETEST']['placement'] == 'below' and self.CurrentSystemVariables['CurrentPrice'] > self.IndicatorsObj['SMA']['value']:
                return

            if self.CurrentSystemVariables['CurrentAccountPositionSize'] < 0:
                if self.CurrentSystemVariables['CurrentPrice'] > self.IndicatorsObj['SMA']['value']:
                    if self.OpenOrderCountInt > 0:
                        self.cancelAllOrders()
                    self.placeMarketOrder('buy')

            elif self.CurrentSystemVariables['CurrentAccountPositionSize'] > 0:
                if self.CurrentSystemVariables['CurrentPrice'] < self.IndicatorsObj['SMA']['value']:
                    if self.OpenOrderCountInt > 0:
                        self.cancelAllOrders()
                    self.placeMarketOrder('sell')

            if self.CurrentSystemVariables['CurrentAccountPositionSize'] == 0:
                PositionSizeFloat = format(self.getOrderQuantity(), '.6f')
                if self.CurrentSystemVariables['CurrentPrice'] > self.IndicatorsObj['SMA']['value']:
                    self.placeMarketOrder('buy', PositionSizeFloat)
                elif self.CurrentSystemVariables['CurrentPrice'] < self.IndicatorsObj['SMA']['value']:
                    self.placeMarketOrder('sell', PositionSizeFloat, True)
        else:
            self.createProcessExecutionLog(self.ProcessName, datetime.utcnow(), "Process Failed: In bbRsiTradingAlgorithmImproved encountered undefined Trading State: " + CurrentTradingStateStr)
        # endregion

    def ema21AnalyzerAlgorithm(self):
        # region Handling actions based on the trading state of the algorithm
        # trading state is managed by the risk management thread
        if not self.checkTradingState():
            return
        # endregion

        if self.IndicatorsObj['EMA_RETEST']['retest_candle_count'] is None:
            return
        if 'LastEmaRetestCount' not in self.CustomVariables:
            self.CustomVariables['LastEmaRetestCount'] = 0

        if 'OpenPositions' not in self.CustomVariables:
            self.CustomVariables['OpenPositions'] = []

        self.OpenOrderCountInt = self.countOpenOrders()
        PositionSizeInt = format(self.getOrderQuantity(), '.6f')

        if self.CurrentSystemVariables['CurrentAccountPositionSize'] != 0 and len(self.CustomVariables['OpenPositions']) == 0:
            if self.CurrentSystemVariables['CurrentAccountPositionSize'] > 0:
                self.CustomVariables['OpenPositions'].append({'OrderSide': 'buy', 'PositionPrice': self.CurrentSystemVariables['CurrentPrice'], 'PositionSize': PositionSizeInt, 'Status': 'New', 'RetestCount': self.CustomVariables['LastEmaRetestCount']})
            else:
                self.CustomVariables['OpenPositions'].append({'OrderSide': 'sell', 'PositionPrice': self.CurrentSystemVariables['CurrentPrice'], 'PositionSize': PositionSizeInt, 'Status': 'New', 'RetestCount': self.CustomVariables['LastEmaRetestCount']})

        if self.CustomVariables['LastEmaRetestCount'] != self.IndicatorsObj['EMA_RETEST']['retest_candle_count']:
            self.CustomVariables['LastEmaRetestCount'] = self.IndicatorsObj['EMA_RETEST']['retest_candle_count']

            for i in range(0, len(self.CustomVariables['OpenPositions'])):
                if self.CustomVariables['OpenPositions'][i]['OrderSide'] == 'buy' and self.IndicatorsObj['EMA'][
                    'value'] <= self.CustomVariables['OpenPositions'][i]['PositionPrice'] and \
                        self.CustomVariables['OpenPositions'][i]['Status'] == 'New':
                    if self.placeMarketOrder('sell', self.CustomVariables['OpenPositions'][i]['PositionSize'], True):
                        self.CustomVariables['OpenPositions'][i]['Status'] = 'Closed'
                        print('Marketed Open Position Sized: ' + str(
                            self.CustomVariables['OpenPositions'][i]['PositionSize']))
                        print('Market Trade Side: ' + 'sell')

                elif self.CustomVariables['OpenPositions'][i]['OrderSide'] == 'sell' and self.IndicatorsObj['EMA'][
                    'value'] >= self.CustomVariables['OpenPositions'][i]['PositionPrice'] and \
                        self.CustomVariables['OpenPositions'][i]['Status'] == 'New':
                    if self.placeMarketOrder('buy', self.CustomVariables['OpenPositions'][i]['PositionSize']):
                        self.CustomVariables['OpenPositions'][i]['Status'] = 'Closed'
                        print('Marketed Open Position Sized: ' + str(
                            self.CustomVariables['OpenPositions'][i]['PositionSize']))
                        print('Market Trade Side: ' + 'buy')

            if self.CurrentSystemVariables['CurrentAccountPositionSize'] != 0:
                self.cancelAllOrders()
                if self.CurrentSystemVariables['CurrentAccountPositionSize'] > 0:
                    self.placeClosingOrder('sell', 'EMA')
                elif self.CurrentSystemVariables['CurrentAccountPositionSize'] < 0:
                    self.placeClosingOrder('buy', 'EMA')

        if self.CustomVariables['LastEmaRetestCount'] < 21:

            if len(self.CustomVariables['OpenPositions']) > 0:
                self.CustomVariables['OpenPositions'] = []
                print('clearing all stored positions')

            if self.OpenOrderCountInt > 0:
                self.cancelAllOrders()

            if self.CurrentSystemVariables['CurrentAccountPositionSize'] != 0:
                print('Marketing all open positions because opened too early')
                if self.CurrentSystemVariables['CurrentAccountPositionSize'] > 0:
                    self.placeMarketOrder('sell', None, True)
                elif self.CurrentSystemVariables['CurrentAccountPositionSize'] < 0:
                    self.placeMarketOrder('buy')
            return

        OpenedPositionsInt = len(self.CustomVariables['OpenPositions'])
        if OpenedPositionsInt > 0 and self.CustomVariables['OpenPositions'][OpenedPositionsInt-1]['RetestCount'] == self.CustomVariables['LastEmaRetestCount']:
            return
        if self.CurrentSystemVariables['CurrentAccountPositionSize'] >= self.CurrentSystemVariables['CurrentPortfolioValue']:
            print('EMA algorithm: too many positions opened. Waiting for them to open up.')
            return

        if self.CurrentSystemVariables['CurrentPrice'] > self.IndicatorsObj['BB']['upper']:
            MarketOrderSideStr = 'sell'
            ClosingOrderSideStr = 'buy'
            BorrowBool = True
        elif self.CurrentSystemVariables['CurrentPrice'] < self.IndicatorsObj['BB']['lower']:
            MarketOrderSideStr = 'buy'
            ClosingOrderSideStr = 'sell'
            BorrowBool = False
        else:
            return

        self.cancelAllOrders()
        self.OpenOrderCountInt = self.countOpenOrders()
        for i in range(0, Constant.RETRY_LIMIT):
            if self.OpenOrderCountInt == 0:
                break
            sleep(0.01)
            self.OpenOrderCountInt = self.countOpenOrders()

        if self.placeMarketOrder(MarketOrderSideStr, PositionSizeInt, BorrowBool):
            self.CustomVariables['OpenPositions'].append(
                {'OrderSide': MarketOrderSideStr, 'PositionPrice': self.CurrentSystemVariables['CurrentPrice'],
                 'PositionSize': PositionSizeInt, 'Status': 'New',
                 'RetestCount': self.CustomVariables['LastEmaRetestCount']})

        for i in range(0, Constant.RETRY_LIMIT):
            if self.CurrentSystemVariables['CurrentAccountPositionSize'] != 0:
                break
            sleep(0.01)

        self.cancelAllOrders()
        self.placeClosingOrder(ClosingOrderSideStr, 'EMA')

        print('New Position Size: ' + str(self.CurrentSystemVariables['CurrentAccountPositionSize']))
        print('Market Order Side: ' + MarketOrderSideStr)
        print('Retest Count: ' + str(self.CustomVariables['LastEmaRetestCount']))

    def priceDataGeneration(self):
        self.getCurrentPrice()
