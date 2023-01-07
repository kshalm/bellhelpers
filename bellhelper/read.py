try:
    import bellhelper.redisHelper as rh
    import bellhelper.streamExceptions as stExcept
except Exception:
    import redisHelper as rh
    import streamExceptions as stExcept
import time
import numpy as np
import math as math

CHANNELCOUNTS = 'monitor:counts'
CHANNELSTATS = 'monitor:stats'
CHANNELVIOLATION = 'monitor:violationstats'
LASTTIMESTAMP = '0-0'
CONFIGKEY = 'config:timetaggers'


def get_power(redis_db, intTime,
              COUNTTYPE='SB',
              COUNTPATH='VV',
              includeNullCounts=False,
              trim=True):
    counts = get_counts(redis_db, intTime=intTime,
                        countPath=COUNTPATH,
                        includeNullCounts=includeNullCounts,
                        trim=trim)[COUNTPATH]
    print(counts)
    if COUNTTYPE == 'SA':
        val = counts[0]
    elif COUNTTYPE == 'Coinc':
        val = counts[1]
    elif COUNTTYPE == 'SB':
        val = counts[2]
    elif COUNTTYPE == 'effA':
        val = counts[3]
    elif COUNTTYPE == 'effB':
        val = counts[4]
    elif COUNTTYPE == 'All':
        val = counts
    else:
        val = counts[5]
    return val


def get_counts(r, intTime=0.2, countPath='VV',
               includeNullCounts=False,
               trim=True, loopArgs={}):
    '''
    r: Redis connection
    intTime: The amount of time to integrate for. This is rounded to the nearest integer multiple
             of 0.2s in the default configuration. So asking for 1.5s of data will actually return 1.6s.
             It depends on what the redis integration time value is set to–if that changes from 0.2s to
             say 0.3s, then the time will be rounded to the nearest integer multiple of 0.3s.
    countPath:  Which path to count from in case there are more than one detector per station.
                'VV' is the default and returns the standard singles/coinc counts in the coinc window.
                'VV_PC' gives the counts in the specified Pockels cell windows
                'VV_Background' give the counts outside the coincidence windows.
    includeNullCounts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained.
    'numTries': The number of attempts to fetch a valid result.
    'trim': Only return results where 'isTrim' is True.
    Returns: Array of [singlesAlice, Coinc, SinglesBob, EfficiencyAlice, EfficiencyBob, EfficiencyAB]
             or returns None if no valid counts obtained.
    '''
    errCheckArgs = {'countPath': countPath,
                    'includeNullCounts': includeNullCounts, 'trim': trim}

    # numTries=numTries, timeOut=timeOut, sleepTime=sleepTime)
    countList = rh.loop_counts(
        r, CHANNELCOUNTS, error_check_counts, errCheckArgs,
        intTime=intTime, **loopArgs)

    if countList is None:
        return None

    countDict = {}
    keys = countList[0].keys()
    for countType in keys:
        if countPath in countType:
            sA = 0
            sB = 0
            coinc = 0
            for c in countList:
                sA += int(c[countType]['As'])
                sB += int(c[countType]['Bs'])
                coinc += int(c[countType]['C'])
            effA, effB, effAB = calc_efficiency(sA, sB, coinc)
            countArray = [sA, coinc, sB, effA, effB, effAB]
            countDict[countType] = countArray

    return countDict


def get_violation(r, intTime=0.2, countPath='VV',
                  includeNullCounts=False,
                  trim=True, loopArgs={}):
    '''
    r: Redis connection
    intTime: The amount of time to integrate for. This is rounded to
                  the nearest integer multiple
             of 0.2s in the default configuration.
             So asking for 1.5s of data will actually return 1.6s.
             It depends on what the redis integration time value
             is set to–if that changes from 0.2s to
             say 0.3s, then the time will be rounded to the nearest
             integer multiple of 0.3s.
    countPath:  Which path to count from in case there are more than
                one detector per station.
                'VV' is the default.
    includeNullCounts: Allow either of the singles counts to be 0 if
                       True.
                       If False waits until a non
                       zero singles is obtained.
    'numTries': The number of attempts to fetch a valid result.
    'trim': Only return results where 'isTrim' is True.
    Returns: A dictionary where each key is a countType that
             contains the countPath and the value is
             a 2D numpy array of all the aggregated counts.
    '''
    errCheckArgs = {'countPath': countPath,
                    'includeNullCounts': includeNullCounts, 'trim': trim}
    loopArgs['intTime'] = intTime

    countList = rh.loop_counts(
        r, CHANNELVIOLATION, error_check_violation, errCheckArgs, **loopArgs)

    if countList is None:
        return None

    countDict = {}
    keys = countList[0].keys()
    for countType in keys:
        if countPath in countType:
            countMatrix = np.zeros((4, 4))
            for c in countList:
                counts = np.array(c[countType])
                countMatrix += counts
            countMatrix = countMatrix.astype(int)
            countDict[countType] = countMatrix
    return countDict


def get_stats(r, intTime=0.5, countPath=('alice', 'bob'),
              includeNullCounts=False, loopArgs={}):
    '''
    r: Redis connection
    intTime: The amount of time to integrate for. This is rounded to the nearest integer multiple
             of 0.2s in the default configuration. So asking for 1.5s of data will actually return 1.6s.
             It depends on what the redis integration time value is set to–if that changes from 0.2s to
             say 0.3s, then the time will be rounded to the nearest integer multiple of 0.3s.
    countPath: The default for this are the parties in a tuple ('alice', 'bob')
    includeNullCounts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained.
    loopArgs = A dictionary of kwargs to be passed to the loop_counts() function.

    Returns: a dictionary with the aggregated counts on each timetagger channel for each party.
             or returns None if no valid counts obtained.
    '''
    errCheckArgs = {'countPath': countPath,
                    'includeNullCounts': includeNullCounts}
    loopArgs['intTime'] = intTime

    countList = rh.loop_counts(
        r, CHANNELSTATS, error_check_stats, errCheckArgs, **loopArgs)

    if countList is None:
        return None

    parties = countPath

    countDict = {}
    for p in parties:
        countDict[p] = np.zeros(8).astype(int)

    for c in countList:
        for p in parties:
            countDict[p] += np.array(c[p]).astype(int)

    return countDict


def calc_efficiency(sA, sB, coinc):
    '''
    Function to compute the efficiencyies given singles and coincide counts.
    '''
    sig = 1
    if (sB == 0) or (sA == 0):
        return [0, 0, 0]
    else:
        effA = 100*coinc*1./sB
        effB = 100*coinc*1./sA
        effAB = 100*coinc*1./(np.sqrt(sA)*np.sqrt(sB))
        eff = [round(effA, sig), round(effB, sig), round(effAB, sig)]
        return eff


def error_check_counts(previousCounts, currentCounts,
                       countPath='VV', includeNullCounts=False,
                       trim=True):
    '''
    Function to make sure that the counts satisfy several conditions. These include 
    the singles not being null (if that option is specified), and that the counts have
    changed from the previous record (makes sure that the timetaggers haven't frozen.)
    previousCounts: the data from the event before the one we are currently considering
    currentCounts: the data from the event we are currently considering
    countPath:  Which path to count from in case there are more than one detector per station. 
                'VV' is the default and returns the standard singles/coinc counts in the coinc window.
                'VV_PC' gives the counts in the specified Pockels cell windows
                'VV_Background' give the counts outside the coincidence windows.
    includeNullCounts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained.
    trim: If true, only accept counts where isTrim=True.
    Returns countsValid: a boolean as to whether the counts are valid or not.
    '''
    countsValid = True

    if trim and currentCounts['isTrim'] is False:
        # Trim check. If required, check that the current value is trimmed. If not, return False
        countsValid = False
        return countsValid

    currentCounts = currentCounts[countPath]
    previousCounts = previousCounts[countPath]

    currentSA = int(currentCounts['As'])
    currentSB = int(currentCounts['Bs'])
    currentCoinc = int(currentCounts['C'])

    previousSA = int(previousCounts['As'])
    previousSB = int(previousCounts['Bs'])
    previousCoinc = int(previousCounts['C'])

    if not includeNullCounts:
        # Null counts in the singles are not valid
        if (currentSA == 0):
            countsValid = False
            nullException = stExcept.nullCountsException('alice')
            raise nullException
        if (currentSB == 0):
            countsValid = False
            nullException = stExcept.nullCountsException('bob')
            raise nullException
        # Make sure that the counts have updated
        if (currentSA == previousSA):
            countsValid = False
            # print(currentCounts, previousCounts)
            # repeatException = stExcept.TimeTaggerRepeatingException('alice')
            # raise repeatException
        if (currentSB == previousSB):
            countsValid = False
            # repeatException = stExcept.TimeTaggerRepeatingException('bob')
            # raise repeatException
    else:
        # Include null counts in the results. Need to catch
        # the condition where one singles rate is 0. In this
        # case we allow it to stay 0 as that isn't necessarily
        # a sign the timetagger server has frozen. If there are
        # singles counts make sure they are updating.
        if (currentSA > 0) and (currentSA == previousSA):
            countsValid = False
            # repeatException = stExcept.TimeTaggerRepeatingException('alice')
            # raise repeatException
        if (currentSB > 0) and (currentSB == previousSB):
            countsValid = False
            # repeatException = stExcept.TimeTaggerRepeatingException('bob')
            # raise repeatException

    return countsValid


def error_check_violation(previousCounts, currentCounts,
                          countPath='VV',
                          includeNullCounts=False, trim=True):
    '''
    Function to make sure that the violation counts satisfy several conditions. These include 
    the singles not being null (if that option is specified), and that the counts have
    changed from the previous record (makes sure that the timetaggers haven't frozen.)
    previousCounts: the data from the event before the one we are currently considering
    currentCounts: the data from the event we are currently considering
    countPath:  Which path to count from in case there are more than one detector per station. 
                'VV' is the default and returns the standard singles/coinc counts in the coinc window.
                'VV_PC' gives the counts in the specified Pockels cell windows
                'VV_Background' give the counts outside the coincidence windows.
    includeNullCounts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained.
    trim: If true, only accept counts where isTrim=True.
    Returns countsValid: a boolean as to whether the counts are valid or not.
    '''
    countsValid = True

    if trim and currentCounts['isTrim'] is False:
        # Trim check. If required, check that
        # the current value is trimmed. If not, return False
        countsValid = False
        return countsValid

    # currentCounts = currentCounts[countPath]
    # previousCounts = previousCounts[countPath]

    currentCountArray = np.array(currentCounts[countPath])
    previousCountArray = np.array(previousCounts[countPath])

    currentSA = currentCountArray[:, 1]
    currentSB = currentCountArray[:, 2]
    previousSA = previousCountArray[:, 1]
    previousSB = previousCountArray[:, 2]

    isAliceNull = np.sum(currentSA) < 1
    isBobNull = np.sum(currentSB) < 1

    # Check and see if we have repeated counts.
    # True means counts have not updated.
    doesAliceRepeat = np.sum((currentSA-previousSA) != 0) == 0
    doesBobRepeat = np.sum((currentSB-previousSB) != 0) == 0

    if not includeNullCounts:
        # Null counts in the singles are not valid
        if isAliceNull:
            countsValid = False
            nullException = stExcept.nullCountsException('alice')
            raise nullException
        if isBobNull:
            countsValid = False
            nullException = stExcept.nullCountsException('bob')
            raise nullException
        # Make sure that the counts have updated
        if doesAliceRepeat:
            countsValid = False
            repeatException = stExcept.TimeTaggerRepeatingException('alice')
            raise repeatException
        if doesBobRepeat:
            countsValid = False
            # repeatException = stExcept.TimeTaggerRepeatingException('bob')
            # raise repeatException
    else:
        # Include null counts in the results. Need to catch
        # the condition where one singles rate is 0. In this
        # case we allow it to stay 0 as that isn't necessarily
        # a sign the timetagger server has frozen. If there are
        # singles counts make sure they are updating.
        if (not isAliceNull) and doesAliceRepeat:
            countsValid = False
            repeatException = stExcept.TimeTaggerRepeatingException('alice')
            raise repeatException
        if (not isBobNull) and doesBobRepeat:
            countsValid = False
            # repeatException = stExcept.TimeTaggerRepeatingException('bob')
            # raise repeatException

    return countsValid


def error_check_stats(previousCounts, currentCounts,
                      countPath='', includeNullCounts=False):
    '''
    Function to make sure that the counts satisfy several conditions. These include 
    the singles not being null (if that option is specified), and that the counts have
    changed from the previous record (makes sure that the timetaggers haven't frozen.)
    previousCounts: the data from the event before the one we are currently considering
    currentCounts: the data from the event we are currently considering
    countPath: The default for this are the parties in a tuple ('alice', 'bob')
    includeNullCounts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained.
    Returns countsValid: a boolean as to whether the counts are valid or not.
    '''
    countsValid = True
    parties = countPath
    for p in parties:
        currentArray = np.array(currentCounts[p]).astype(int)
        previousArray = np.array(previousCounts[p]).astype(int)
        if np.sum(currentArray) == np.sum(previousArray):
            countsValid = False
            repeatException = stExcept.TimeTaggerRepeatingException(p)
            raise repeatException
        if np.sum(currentArray) == 0:
            ttagException = stExcept.nullCountsTimeTaggerException(p)
            raise ttagException
    return countsValid


def set_integration_time(r, intTime, configKey):
    '''
    Function to change the integration time.
    r: Redis connection
    intTime:    The new integration time
    configKey:  The redis stream containing the configuration dictionary
    Returns:    The previous integration time.
    '''
    config = rh.get_config(r, configKey)
    currentIntTime = config['INT_TIME']
    if float(currentIntTime) != float(intTime):
        config['INT_TIME'] = float(intTime)
        rh.set_config(r, config, configKey)

    return currentIntTime


def get_integration_time(r, configKey=None):
    '''
    Function to fetch the integration time.
    r: Redis connection
    configKey:  The redis stream containing the configuration dictionary
    Returns:    The current integration time.
    '''
    if configKey is None:
        configKey = CONFIGKEY
    config = rh.get_config(r, configKey)
    currentIntTime = config['INT_TIME']
    return currentIntTime


def test_stream(r, nTimes):
    global LASTTIMESTAMP
    t1 = time.time()
    LASTTIMESTAMP = '0-0'

    msgCounts = rh.get_data(r, CHANNELCOUNTS, LASTTIMESTAMP)
    if msgCounts is not None:
        LASTTIMESTAMP = msgCounts[-1][0]
        counts = msgCounts[-1][1]
    i = 0
    while i < nTimes:
        msgCounts = rh.get_data(r, CHANNELCOUNTS, LASTTIMESTAMP)
        if msgCounts is not None:
            LASTTIMESTAMP = msgCounts[-1][0]
            counts = msgCounts[-1][1]
            i += 1
            t2 = time.time()
            print('Success', t2-t1)


if __name__ == '__main__':
    redisIP = 'bellamd1.campus.nist.gov'
    redisPort = 6379
    db = 0
    redisConfig = {'ip': redisIP, 'port': redisPort, 'db': db}
    r = rh.connect_to_redis(redisConfig)

    # oldIntegrationTime = set_integration_time(r, 0.5, CONFIGKEY)
    # print('old integration time', oldIntegrationTime)
    loopArgs = {}
    loopArgs['numTries'] = 100
    loopArgs['timeOut'] = 10

    # ut = rh.stream_last_updated(r, CHANNELCOUNTS)
    # alive, lastUpdate, lastItem= rh.is_stream_alive(r, CHANNELCOUNTS, 0.4)
    # print(alive, lastUpdate, lastItem)

    countsArray = ''
    try:
        # countsArray = get_counts(r, intTime = 1., countPath='VV',
        #     includeNullCounts=False, trim=False, loopArgs=loopArgs)

        # countsArray = get_violation(r, intTime = 1., countPath='VV',
        #     includeNullCounts=False, trim=False, loopArgs=loopArgs)

        countsArray = get_stats(
            r, intTime=1., includeNullCounts=False, loopArgs=loopArgs)

    except stExcept.StreamException as e:
        print(e)
    except (stExcept.StreamFrozenException,
            stExcept.streamTimeoutException) as e:
        print(e)

    print(countsArray)

    # set_integration_time(r, oldIntegrationTime, CONFIGKEY)

    # configKey = 'config:timetaggers'
    # config = rh.get_config(r, configKey)
    # print(config)

    # test_stream(r,10)
