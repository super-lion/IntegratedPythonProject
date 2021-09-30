from FrameworkBaseClasses.IndicatorGenerationBaseClass import IndicatorGenerationBaseClass
from assets import constants as Constant

from datetime import datetime


class IndicatorGenerationClass(IndicatorGenerationBaseClass):
    def __init__(self):
        # print("Indicator Generation Class Constructor")
        self.ObjectTypeValidationArr = {
            'SystemVariables': ['AlgorithmId', 'CurrentPrice', 'CurrentAccountBalance', 'CurrentPortfolioValue', 'TradingState'],
            'DatabaseDetails': ['ServerName', 'DatabaseName', 'UserName', 'Password'],
            'Indicators': ['BB', 'RSI', 'SMA', 'EMA', 'EMA_RETEST', 'TimeStamp', 'COC'],
            'CandleArr': ['FiveMinuteCandles']
        }
        super().__init__()

    def initiateExecution(self):
        self.updateCandleArr()
        SelectedAlgorithmNameStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]

        if SelectedAlgorithmNameStr == Constant.BB_RSI_ALGORITHM_V1 or \
                SelectedAlgorithmNameStr == Constant.BB_RSI_ALGORITHM_V2 or \
                SelectedAlgorithmNameStr == Constant.BB_RSI_ALGORITHM_V3:
            self.updateIndicatorsForBbRsiAlgorithm()
        elif SelectedAlgorithmNameStr == Constant.EMA_ALGORITHM_V1:
            self.updateIndicatorsForEma21AnalyserAlgorithm()

        self.IndicatorsObj['TimeStamp']['datetime'] = datetime.utcnow()

    def updateIndicatorsForBbRsiAlgorithm(self):
        self.updateBollingerBandIndicator()
        self.updateRsiBandIndicator()
        self.updateSmaIndicator()
        self.updateEmaIndicator()
        self.updateEmaRetestIndicator()
        self.updateClosingOrderCountIndicator()

    def updateIndicatorsForEma21AnalyserAlgorithm(self):
        self.updateBollingerBandIndicator()
        self.updateEmaIndicator()
        self.updateEmaRetestIndicator()
