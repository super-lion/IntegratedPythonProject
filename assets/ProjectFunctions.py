from assets import constants as Constant

import math


def getBollingerBands(CandleDataArr, AlgorithmConfigurationObj):
    # print("get Bollinger Bands")
    BollingerBandStandardDeviationFloat = float(AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_BB_STANDARD_DEVIATION_INDEX])
    SimpleMovingAverageFloat = getSimpleMovingAverage(CandleDataArr)
    StandardDeviationFloat = getStandardDeviationOfCandleArr(CandleDataArr)
    UpperBandFloat = SimpleMovingAverageFloat['value'] + (StandardDeviationFloat * BollingerBandStandardDeviationFloat)
    LowerBandFloat = SimpleMovingAverageFloat['value'] - (StandardDeviationFloat * BollingerBandStandardDeviationFloat)
    BollingerBandObj = {
        'upper': UpperBandFloat,
        'lower': LowerBandFloat
    }

    return BollingerBandObj


def getRsiBands(CandleDataArr, AlgorithmConfigurationObj):
    # print("get RSI Bands")
    RsiUpperBandIntensityFloat = float(AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_RSI_UPPER_INTENSITY_INDEX])
    RsiLowerBandIntensityFloat = float(AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_RSI_LOWER_INTENSITY_INDEX])
    IndicatorFrameCount = int(AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_FRAME_COUNT_INDEX])
    ArrLengthInt = len(CandleDataArr)
    CandlePositiveChangeArr = []
    CandleNegativeChangeArr = []

    for CandleObj in CandleDataArr:
        if CandleObj['open'] <= CandleObj['close']:
            CandlePositiveChangeArr.append(CandleObj['close'] - CandleObj['open'])
        else:
            CandleNegativeChangeArr.append(CandleObj['open'] - CandleObj['close'])
    AveragePositiveChangeFloat = 0
    AverageNegativeChangeFloat = 0

    if len(CandlePositiveChangeArr) > 0:
        AveragePositiveChangeFloat = sum(CandlePositiveChangeArr)/ArrLengthInt
    if len(CandleNegativeChangeArr) > 0:
        AverageNegativeChangeFloat = sum(CandleNegativeChangeArr)/ArrLengthInt

    SimpleMovingAverageFloat = getSimpleMovingAverage(CandleDataArr)

    AverageGain = (AveragePositiveChangeFloat/RsiUpperBandIntensityFloat) * \
                  (100-RsiUpperBandIntensityFloat)

    AverageLoss = (AverageNegativeChangeFloat * RsiLowerBandIntensityFloat) / \
                  (100 - RsiLowerBandIntensityFloat)

    UpperBandFloat = SimpleMovingAverageFloat['value'] + AverageGain * IndicatorFrameCount
    LowerBandFloat = SimpleMovingAverageFloat['value'] - AverageLoss * IndicatorFrameCount

    RsiBandObj = {
        'upper': UpperBandFloat,
        'lower': LowerBandFloat
    }

    return RsiBandObj


def getSimpleMovingAverage(CandleDataArr):
    # print("get Simple Moving Average")
    CandleArrSumFloat = 0
    for CandleObj in CandleDataArr:
        CandleArrSumFloat += CandleObj['mid']
    SimpleMovingAverageFloat = CandleArrSumFloat / len(CandleDataArr)

    SimpleMovingAverageObj = {
        'value': SimpleMovingAverageFloat
    }
    return SimpleMovingAverageObj


def getExponentialMovingAverage(CandleDataArr, FrameCountInt, PrevExponentialMovingAverageFloat, SmoothingFactorFloat):
    CurrentValueFloat = CandleDataArr[len(CandleDataArr)-1]['mid']
    ExponentialMovingAverageFloat = (CurrentValueFloat * (SmoothingFactorFloat / (1 + FrameCountInt))) + (PrevExponentialMovingAverageFloat * (1 - (SmoothingFactorFloat / (1 + FrameCountInt))))
    ExponentialMovingAverageObj = {
        'value': ExponentialMovingAverageFloat
    }
    return ExponentialMovingAverageObj


def getStandardDeviationOfCandleArr(CandleDataArr):
    # print("get StandardDeviation Of Candle Arr")
    VarianceFloat = 0.0
    CandleArrSumFloat = 0
    for CandleObj in CandleDataArr:
        CandleArrSumFloat += CandleObj['mid']

    AverageFloat = CandleArrSumFloat/len(CandleDataArr)

    for CandleObj in CandleDataArr:
        VarianceFloat += pow(float(CandleObj['mid']) - float(AverageFloat), 2)

    return float(math.sqrt(VarianceFloat/len(CandleDataArr)))


def checkIfNumber(MixedVariable):
    if type(MixedVariable) == int or type(MixedVariable) == float:
        return True
    else:
        return False


def truncateFloat(InputFloat, DecimalPlacesInt):
    s = '{}'.format(InputFloat)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(InputFloat, DecimalPlacesInt)
    i, p, d = s.partition('.')
    return float('.'.join([i, (d + '0' * DecimalPlacesInt)[:DecimalPlacesInt]]))
