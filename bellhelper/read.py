try:
    import bellhelper.redisHelper as rh
except:
    import redisHelper as rh
import time
import numpy as np
import math as math

CHANNELCOUNTS = 'monitor:counts'
LASTTIMESTAMP = '0-0'
CONFIGKEY = 'config:timetaggers'
# DEFAULTINTTIME = 0.2


def get_counts(r, intTime=0.2, countPath='VV', numTries=-1, inlcudeNullCounts=False, trim=True):
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
    global LASTTIMESTAMP, DEFAULTINTTIME
    if numTries == 0:
        numTries = 1

    t1 = time.time()
    
    msgCounts = rh.get_data(r, CHANNELCOUNTS, LASTTIMESTAMP, count=1)
    if msgCounts is not None:
        LASTTIMESTAMP = msgCounts[-1][0]
        counts = msgCounts[-1][1]
        defaultIntegrationTime = counts['integrationTime']

    nSamples = int(math.ceil(float(intTime)/float(defaultIntegrationTime)))

    intTime = get_integration_time(r, CONFIGKEY)
    cont = True

    i = 0
    countList = []
    defaultNumTries = numTries

    

    for j in range(nSamples):
        cont = True
        numTries = defaultNumTries
        while cont:
            time.sleep(.1)
            msgCounts = rh.get_data(r, CHANNELCOUNTS, LASTTIMESTAMP, count=2)
            if msgCounts is not None:
                newTimeStamp = msgCounts[-1][0]
                counts = msgCounts[-1][1]
            else:
                continue
            isTrim = counts['isTrim']
            currentIntegrationTime = counts['integrationTime']
            isCorrectIntegrationTime = float(
                defaultIntegrationTime) == float(currentIntegrationTime)
            countsValid = error_check(msgCounts, countPath, inlcudeNullCounts)
            if countsValid and isCorrectIntegrationTime:
                if not trim or (trim and isTrim):
                    LASTTIMESTAMP = newTimeStamp
                    cont = False
                    countList.append(counts[countPath])
                    t2 = time.time()
                    # print(j, 'Elapsed time:', t2-t1, intTime)
                    break  # end the loop

            if (i >= numTries-1) and (numTries > 0):
                cont = False
                return None  # No valid answer
            i += 1
            

    # t2 = time.time()
    # print('Elapsed time:', t2-t1, intTime)
    sA = 0
    sB = 0 
    coinc = 0 
    for c in countList:
        sA += int(c['As'])
        sB += int(c['Bs'])
        coinc += int(c['C'])
    
    effA, effB, effAB = calc_efficiency(sA, sB, coinc)
    countArray = [sA, coinc, sB, effA, effB, effAB]

    return countArray


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


def error_check(msgCounts, countPath, inlcudeNullCounts=False):
    '''
    Function to make sure that the counts satisfy several conditions. These include 
    the singles not being null (if that option is specified), and that the counts have
    changed from the previous record (makes sure that the timetaggers haven't frozen.)
    msgCounts:  The raw data from the counts Redis stream. There should 
                be two elements here.
    countPath:  Which path to count from in case there are more than one detector per station. 
                'VV' is the default and returns the standard singles/coinc counts in the coinc window.
                'VV_PC' gives the counts in the specified Pockels cell windows
                'VV_Background' give the counts outside the coincidence windows.
    includeNullCounts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained.
    Returns countsValid: a boolean as to whether the counts are valid or not.
    '''
    countsValid = True
    # print('msg len', len(msgCounts))
    if len(msgCounts) > 1:
        currentCounts = msgCounts[-1][1][countPath]
        previousCounts = msgCounts[-2][1][countPath]

        currentSA = int(currentCounts['As'])
        currentSB = int(currentCounts['Bs'])
        currentCoinc = int(currentCounts['C'])

        previousSA = int(previousCounts['As'])
        previousSB = int(previousCounts['Bs'])
        previousCoinc = int(previousCounts['C'])

        if not inlcudeNullCounts:
            # Null counts in the singles are not valid
            if (currentSA == 0) or (currentSB == 0):
                countsValid = False
            if (currentSA == previousSA) or (currentSB == previousSB):
                # Make sure that the counts have updated
                countsValid = False
        else:
            # Include null counts in the results. Need to catch
            # the condition where one singles rate is 0. In this
            # case we allow it to stay 0 as that isn't necessarily
            # a sign the timetagger server has frozen. If there are
            # singles counts make sure they are updating.
            if (currentSA > 0) and (currentSA == previousSA):
                countsValid = False
            if (currentSB > 0) and (currentSB == previousSB):
                countsValid = False
    else:
        countsValid = False

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


if __name__ == '__main__':
    redisIP = 'bellamd1.campus.nist.gov'
    redisPort = 6379
    db = 0
    redisConfig = {'ip': redisIP, 'port': redisPort, 'db': db}
    r = rh.connect_to_redis(redisConfig)

    # oldIntegrationTime = set_integration_time(r, .2, CONFIGKEY)
    # print('old integration time', oldIntegrationTime)
    countsArray = get_counts(r, intTime = 1., countPath='VV', numTries=-1, 
        inlcudeNullCounts=True, trim=True)
    print(countsArray)
    # set_integration_time(r, oldIntegrationTime, CONFIGKEY)

    # configKey = 'config:timetaggers'
    # config = rh.get_config(r, configKey)
    # print(config)
