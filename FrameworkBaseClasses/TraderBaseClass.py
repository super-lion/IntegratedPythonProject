from FrameworkBaseClasses.ProcessBaseClass import ProcessBaseClass
from assets import ProjectFunctions
from assets import constants as Constant

import math
import ccxt
from datetime import datetime
import mysql.connector
import time


class TraderBaseClass(ProcessBaseClass):
    OpenOrderCountInt = 0
    OpenPositionCountInt = 0
    CurrentOrderArr = []
    MarginTradingCurrency = 'BTC'
    CustomVariables = {}

    def __init__(self):
        # print("Trader Base Class Constructor")
        self.ProcessName = "Trader Process"
        self.ObjectTypeValidationArr = {
            'Indicators': ['BB', 'RSI', 'SMA', 'EMA_RETEST'],
            'SystemVariables': [
                'CurrentPrice',
                'CurrentAccountBalance',
                'CurrentPortfolioValue',
                'SystemState',
                'TradingState'
            ],
            'DatabaseDetails': ['ServerName', 'DatabaseName', 'UserName', 'Password'],
            'ExchangeDetails': ['ExchangeName', 'ApiKey', 'ApiSecret']
        }

        super().__init__()

    def countOpenOrders(self):
        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                self.CurrentOrderArr = self.ExchangeConnectionObj.fetch_open_orders(
                    self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                )
                return len(self.CurrentOrderArr)
            except ccxt.NetworkError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "fetch_open_orders("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ")", "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "fetch_open_orders("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ")", "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "fetch_open_orders("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ")", "OtherError: " + str(ErrorMessage)
                )
        return False

    def cancelAllOrders(self):
        CancelFailedBool = False
        for CurrentOrder in self.CurrentOrderArr:
            for iterator in range(0, Constant.RETRY_LIMIT):
                try:
                    if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                        self.ExchangeConnectionObj.cancel_order(CurrentOrder['id'], CurrentOrder['symbol'])
                    else:
                        self.ExchangeConnectionObj.cancel_order(CurrentOrder['id'])
                    break
                except ccxt.NetworkError as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT-1:
                        continue
                    self.createExchangeInteractionLog(
                        self.ProcessName,
                        datetime.utcnow(),
                        "cancel_order(" + CurrentOrder['id'] + ")",
                        "NetworkError: " + str(ErrorMessage)
                    )
                    CancelFailedBool = True
                except ccxt.ExchangeError as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT-1:
                        continue
                    self.createExchangeInteractionLog(
                        self.ProcessName,
                        datetime.utcnow(),
                        "cancel_order(" + CurrentOrder['id'] + ")",
                        "ExchangeError: " + str(ErrorMessage)
                    )
                    CancelFailedBool = True
                except Exception as ErrorMessage:
                    if iterator != Constant.RETRY_LIMIT-1:
                        continue
                    self.createExchangeInteractionLog(
                        self.ProcessName,
                        datetime.utcnow(),
                        "cancel_order(" + CurrentOrder['id'] + ")",
                        "OtherError: " + str(ErrorMessage)
                    )
                    CancelFailedBool = True
        return not CancelFailedBool

    def placeClosingOrder(self, OrderSideStr, IndicatorNameStr='SMA'):
        OrderParameterObj = {}
        OrderQuantityInt = format(abs(self.CurrentSystemVariables['CurrentAccountPositionSize']), '.8f')
        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                    self.ExchangeConnectionObj.sapi_post_margin_order({
                        'symbol': 'BTCUSDT',
                        'side': OrderSideStr.upper(),
                        'type': 'LIMIT',
                        'quantity': OrderQuantityInt,
                        'price': round(self.IndicatorsObj[IndicatorNameStr]['value']),
                        'sideEffectType': 'MARGIN_BUY',
                        'timeInForce': 'GTC',
                        'timestamp': str(round(time.time() * 1000))
                    })
                else:
                    self.ExchangeConnectionObj.create_order(
                        self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                        'limit',
                        OrderSideStr,
                        OrderQuantityInt,
                        round(self.IndicatorsObj[IndicatorNameStr]['value']),
                        OrderParameterObj
                    )
                self.createOrderLog(
                    datetime.utcnow(),
                    round(self.IndicatorsObj[IndicatorNameStr]['value']),
                    'close',
                    OrderSideStr,
                    OrderQuantityInt,
                    self.CurrentSystemVariables['TradingState']
                )
                break
            except ccxt.NetworkError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'limit'," +
                    OrderSideStr + "," + str(OrderQuantityInt) + ", " +
                    str(round(self.IndicatorsObj[IndicatorNameStr]['value'])) + ")",
                    "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'limit'," +
                    OrderSideStr + "," + str(OrderQuantityInt) + ", " +
                    str(round(self.IndicatorsObj[IndicatorNameStr]['value'])) + ")",
                    "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'limit'," +
                    OrderSideStr + "," + str(OrderQuantityInt) + ", " +
                    str(round(self.IndicatorsObj[IndicatorNameStr]['value'])) + ")",
                    "OtherError: " + str(ErrorMessage)
                )

    def placeOpeningOrders(self):
        UpperLimitArr = [self.IndicatorsObj['BB']['upper'], self.IndicatorsObj['RSI']['upper']]
        LowerLimitArr = [self.IndicatorsObj['BB']['lower'], self.IndicatorsObj['RSI']['lower']]
        OrderQuantityInt = format(self.getOrderQuantity(), '.6f')

        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                OrderSideStr = 'sell'
                if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                    self.ExchangeConnectionObj.sapi_post_margin_order({
                        'symbol': 'BTCUSDT',
                        'side': OrderSideStr.upper(),
                        'type': 'LIMIT',
                        'quantity': OrderQuantityInt,
                        'price': round(max(UpperLimitArr)),
                        'sideEffectType': 'MARGIN_BUY',
                        'timeInForce': 'GTC',
                        'timestamp': str(round(time.time() * 1000))
                    })
                else:
                    self.ExchangeConnectionObj.create_order(
                        self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                        'limit', OrderSideStr,
                        OrderQuantityInt,
                        round(max(UpperLimitArr))
                    )
                self.createOrderLog(
                    datetime.utcnow(),
                    round(max(UpperLimitArr)),
                    'open',
                    OrderSideStr,
                    OrderQuantityInt,
                    self.CurrentSystemVariables['TradingState']
                )
                break
            except ccxt.NetworkError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'limit', 'sell', " + str(OrderQuantityInt) + "," +
                    str(round(max(UpperLimitArr))) + ")",
                    "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'limit', 'sell', " + str(OrderQuantityInt) + "," +
                    str(round(max(UpperLimitArr))) + ")",
                    "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'limit', 'sell', " + str(OrderQuantityInt) + "," +
                    str(round(max(UpperLimitArr))) + ")",
                    "OtherError: " + str(ErrorMessage)
                )

        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                OrderSideStr = 'buy'
                if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                    self.ExchangeConnectionObj.sapi_post_margin_order({
                        'symbol': 'BTCUSDT',
                        'side': OrderSideStr.upper(),
                        'type': 'LIMIT',
                        'quantity': OrderQuantityInt,
                        'price': round(min(LowerLimitArr)),
                        'sideEffectType': 'MARGIN_BUY',
                        'timeInForce': 'GTC',
                        'timestamp': str(round(time.time() * 1000))
                    })
                else:
                    self.ExchangeConnectionObj.create_order(
                        self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                        'limit', OrderSideStr,
                        OrderQuantityInt,
                        round(min(LowerLimitArr))
                    )
                self.createOrderLog(
                    datetime.utcnow(),
                    round(min(LowerLimitArr)),
                    'open',
                    OrderSideStr,
                    OrderQuantityInt,
                    self.CurrentSystemVariables['TradingState']
                )
                break
            except ccxt.NetworkError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'limit', 'buy', " + str(OrderQuantityInt) + "," +
                    str(round(min(LowerLimitArr))) + ")",
                    "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'limit', 'buy', " + str(OrderQuantityInt) + "," +
                    str(round(min(LowerLimitArr))) + ")",
                    "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'limit', 'buy', " + str(OrderQuantityInt) + "," +
                    str(round(min(LowerLimitArr))) + ")",
                    "OtherError: " + str(ErrorMessage)
                )

    def placeMarketOrder(self, OrderSideStr, QuantityInt=None, BorrowBool=False):
        if QuantityInt is None:
            QuantityInt = abs(self.CurrentSystemVariables['CurrentAccountPositionSize'])
        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                    if BorrowBool:
                        self.ExchangeConnectionObj.sapi_post_margin_order({
                            'symbol': 'BTCUSDT',
                            'side': OrderSideStr.upper(),
                            'type': 'MARKET',
                            'quantity': QuantityInt,
                            'sideEffectType': 'MARGIN_BUY',
                            'timestamp': str(round(time.time() * 1000))
                        })
                    else:
                        self.ExchangeConnectionObj.sapi_post_margin_order({
                            'symbol': 'BTCUSDT',
                            'side': OrderSideStr.upper(),
                            'type': 'MARKET',
                            'quantity': QuantityInt,
                            'timestamp': str(round(time.time() * 1000))
                        })
                else:
                    self.ExchangeConnectionObj.create_order(
                        self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                        'market',
                        OrderSideStr,
                        QuantityInt,
                        {'type': 'market'}
                    )
                self.createOrderLog(
                    datetime.utcnow(),
                    self.CurrentSystemVariables['CurrentPrice'],
                    'market',
                    OrderSideStr,
                    QuantityInt,
                    self.CurrentSystemVariables['TradingState']
                )

                return True
            except ccxt.NetworkError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'market'," +
                    OrderSideStr + "," + str(QuantityInt) + ", " +
                    "market" + ")",
                    "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'market'," +
                    OrderSideStr + "," + str(QuantityInt) + ", " +
                    "market" + ")",
                    "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", 'market'," +
                    OrderSideStr + "," + str(QuantityInt) + ", " +
                    "market" + ")",
                    "OtherError: " + str(ErrorMessage)
                )

        return False

    def getOrderQuantity(self):
        AlgorithmExposureFloat = float(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EXPOSURE_INDEX])
        if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
            return float(self.CurrentSystemVariables['CurrentPortfolioValue']) * AlgorithmExposureFloat / \
                   self.CurrentSystemVariables['CurrentPrice']
        elif self.ExchangeConnectionDetails['ExchangeName'] == Constant.BITMEX_EXCHANGE_ID:
            CurrentPositionObj = self.ExchangeConnectionObj.private_get_position()
            return CurrentPositionObj[0]['currentQty']

    def getCurrentPrice(self):
        TradingPairSymbolStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                self.CurrentSystemVariables['CurrentPrice'] = self.ExchangeConnectionObj.fetch_ticker(TradingPairSymbolStr)['bid']
                self.createPriceLogEntry(datetime.utcnow(), self.CurrentSystemVariables['CurrentPrice'])
                break
            except Exception as ErrorMessage:
                # Please create a log table and a log function for exchange related retrievals.
                # We will only log errors in this table
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "WebSocket get_ticket()['mid]", ErrorMessage
                )

    def checkTradingState(self):
        if self.CurrentSystemVariables['TradingState'] == 'Market Halt':
            if self.CurrentSystemVariables['CurrentAccountPositionSize'] == 0 and self.OpenOrderCountInt > 0:
                self.cancelAllOrders()
                self.createProcessExecutionLog(self.ProcessName, datetime.utcnow(),
                                               "Process Update: Closing all orders on Market Halt trading state")
                return False
            elif self.OpenOrderCountInt == 0 and self.CurrentSystemVariables['CurrentAccountPositionSize'] == 0:
                return False
        elif self.CurrentSystemVariables['TradingState'] == 'Market Dead Stop' or \
                self.CurrentSystemVariables['TradingState'] == 'Manual Halt':
            if self.OpenOrderCountInt > 0:
                self.cancelAllOrders()
            if self.CurrentSystemVariables['CurrentAccountPositionSize'] != 0:
                if self.CurrentSystemVariables['CurrentAccountPositionSize'] > 0:
                    self.placeMarketOrder('sell')
                elif self.CurrentSystemVariables['CurrentAccountPositionSize'] < 0:
                    self.placeMarketOrder('buy')

                self.createProcessExecutionLog(self.ProcessName, datetime.utcnow(),
                                               "Process Update: Creating market orders on open position due to "
                                               + self.CurrentSystemVariables['TradingState'] + " trading state")
                if self.OpenOrderCountInt > 0:
                    self.cancelAllOrders()
                    self.createProcessExecutionLog(self.ProcessName, datetime.utcnow(),
                                                   "Process Update: Closing all orders due to "
                                                   + self.CurrentSystemVariables['TradingState'] + " trading state")
            return False
        elif self.CurrentSystemVariables['TradingState'] is None:
            # In case the algorithm configuration variables are not set yet, we do not execute trading functionality
            self.createProcessExecutionLog(self.ProcessName, datetime.utcnow(),
                                           "Process Update: Algorithm trading state not set")
            return False

        return True

    def genericPlaceLimitTrade(self, PayloadObj):
        # region Handling actions based on the trading state of the algorithm
        # trading state is managed by the risk management thread
        if not self.checkTradingState():
            return
        # endregion

        if PayloadObj['TradeAction'] == 'close':
            OrderQuantityInt = self.CurrentSystemVariables['CurrentAccountPositionSize']
        else:
            OrderQuantityInt = format(self.getOrderQuantity(), '.6f')

        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                self.ExchangeConnectionObj.sapi_post_margin_order({
                    'symbol': self.AlgorithmConfigurationObj[
                        Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX].replace('/', ''),
                    'side': PayloadObj['TradeDirection'],
                    'type': PayloadObj['TradeType'],
                    'quantity': OrderQuantityInt,
                    'price': PayloadObj['Price'],
                    'sideEffectType': PayloadObj['SideEffect'],
                    'timeInForce': 'GTC',
                    'timestamp': str(round(time.time() * 1000))
                })

                self.createOrderLog(
                    datetime.strptime(PayloadObj['Time'], '%Y-%m-%dT%H:%M:%SZ'),
                    PayloadObj['Price'],
                    PayloadObj['TradeAction'],
                    PayloadObj['TradeDirection'],
                    OrderQuantityInt,
                    'N/A'
                )
                # print('Limit Trade Placed')
                break
            except ccxt.NetworkError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT - 1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", " + PayloadObj['TradeType'] + ", " + PayloadObj['TradeDirection'] + ", "
                    + str(OrderQuantityInt) + "," + PayloadObj['Price'] + ")",
                    "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT - 1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", " + PayloadObj['TradeType'] + ", " + PayloadObj['TradeDirection'] + ", "
                    + str(OrderQuantityInt) + "," + PayloadObj['Price'] + ")",
                    "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT - 1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", " + PayloadObj['TradeType'] + ", " + PayloadObj['TradeDirection'] + ", "
                    + str(OrderQuantityInt) + "," + PayloadObj['Price'] + ")",
                    "OtherError: " + str(ErrorMessage)
                )

    def genericPlaceMarketTrade(self, PayloadObj):
        # region Handling actions based on the trading state of the algorithm
        # trading state is managed by the risk management thread
        if not self.checkTradingState():
            return
        # endregion
        if PayloadObj['TradeAction'] == 'close':
            OrderQuantityInt = self.CurrentSystemVariables['CurrentAccountPositionSize']
        else:
            OrderQuantityInt = format(self.getOrderQuantity(), '.6f')

        for iterator in range(0, Constant.RETRY_LIMIT):
            try:
                self.ExchangeConnectionObj.sapi_post_margin_order({
                    'symbol': self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX].replace('/', ''),
                    'side': PayloadObj['TradeDirection'],
                    'type': PayloadObj['TradeType'],
                    'quantity': OrderQuantityInt,
                    'sideEffectType': PayloadObj['SideEffect'],
                    'timestamp': str(round(time.time() * 1000))
                })

                self.createOrderLog(
                    datetime.strptime(PayloadObj['Time'], '%Y-%m-%dT%H:%M:%SZ'),
                    PayloadObj['Price'],
                    PayloadObj['TradeAction'],
                    PayloadObj['TradeDirection'],
                    OrderQuantityInt,
                    'N/A'
                )
                # print('Market Trade Placed')
                break
            except ccxt.NetworkError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", " + PayloadObj['TradeType'] + ", " + PayloadObj['TradeDirection'] + ", "
                    + str(OrderQuantityInt) + "," + PayloadObj['Price'] + ")",
                    "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", " + PayloadObj['TradeType'] + ", " + PayloadObj['TradeDirection'] + ", "
                    + str(OrderQuantityInt) + "," + PayloadObj['Price'] + ")",
                    "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                if iterator != Constant.RETRY_LIMIT-1:
                    continue
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.utcnow(),
                    "create_order("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + ", " + PayloadObj['TradeType'] + ", " + PayloadObj['TradeDirection'] + ", "
                    + str(OrderQuantityInt) + "," + PayloadObj['Price'] + ")",
                    "OtherError: " + str(ErrorMessage)
                )
