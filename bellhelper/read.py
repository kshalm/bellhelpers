try:
    import bellhelper.redisHelper as rh
except:
    import redisHelper as rh
import time
import numpy as np
import math as math

CHANNELCOUNTS = 'monitor:counts'
CHANNELSTATS = 'monitor:stats'
CHANNELVIOLATION = 'monitor:violationstats'
LASTTIMESTAMP = '0-0'
CONFIGKEY = 'config:timetaggers'

def loop_counts(r, channel, error_function, intTime=0.2, inlcudeNullCounts=False,
                 countPath='VV', numTries=-1, trim=True):
    if numTries == 0:
        numTries = 1

    msgCounts = rh.get_last_entry(r, channel, count=2)
    if msgCounts is not None:
        # Need to grab the second to last timestamp to start with.
        # We need the current and last 
        LASTTIMESTAMP = msgCounts[0][0]
        counts = msgCounts[1][1]
        defaultIntegrationTime = counts['integrationTime']

    nSamples = int(math.ceil(float(intTime)/float(defaultIntegrationTime)))
    cont = True

    i = 0
    countList = []
    defaultNumTries = numTries
    t1 = time.time()

    j=0
    while j<nSamples:
        cont = True
        numTries = defaultNumTries
        i = 0
        while cont:
            i+=1
            time.sleep(.05)
            msgCounts = rh.get_data(r, channel, LASTTIMESTAMP)
            t2 = time.time()
            # print(i, j, 'Elapsed time:', t2-t1)
            if (msgCounts is not None) and len(msgCounts)>=2:
                # Need to make sure that an update has occured which requires
                # at least two new entries since the first one.
                pass
            else:
                # print('trying again not enough data')
                if (i >= numTries) and (numTries > 0):
                    cont = False
                    return None 
                continue

            goodCounts, LASTTIMESTAMP = parse_counts(msgCounts, countPath, 
                                        inlcudeNullCounts, trim, error_function)

            if len(goodCounts)==0:
                # print('no good counts')
                continue
            else: 
                countsToAdd = goodCounts[0:nSamples-j]
                j+=len(countsToAdd)
                countList+=countsToAdd
                t2 = time.time()
                # print('SUCCESS',  'Elapsed time:', t2-t1, intTime, LASTTIMESTAMP)
                break  # end the loop
            if (i >= numTries) and (numTries > 0):
                cont = False
                return None  # No valid answer
    return countList


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
    countList = loop_counts(r, CHANNELCOUNTS, error_check, intTime=intTime, 
                inlcudeNullCounts=inlcudeNullCounts, countPath=countPath, 
                numTries=numTries, trim=trim)

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

def get_violation(r, intTime=0.2, countPath='VV', numTries=-1, inlcudeNullCounts=False, trim=True):
    '''
    r: Redis connection
    intTime: The amount of time to integrate for. This is rounded to the nearest integer multiple
             of 0.2s in the default configuration. So asking for 1.5s of data will actually return 1.6s. 
             It depends on what the redis integration time value is set to–if that changes from 0.2s to
             say 0.3s, then the time will be rounded to the nearest integer multiple of 0.3s.
    countPath:  Which path to count from in case there are more than one detector per station. 
                'VV' is the default.
    includeNullCounts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained. 
    'numTries': The number of attempts to fetch a valid result.
    'trim': Only return results where 'isTrim' is True.
    Returns: A dictionary where each key is a countType that contains the countPath and the value is
                a 2D numpy array of all the aggregated counts.
    '''     
    countList = loop_counts(r, CHANNELVIOLATION, error_check_violation, intTime=intTime, 
                inlcudeNullCounts=inlcudeNullCounts, countPath=countPath, 
                numTries=numTries, trim=trim)

    if countList is None:
        return None

    countDict = {}
    keys = countList[0].keys()
    for countType in keys:
        if countPath in countType:
            countMatrix = np.zeros((4,4)) 
            for c in countList:
                counts = np.array(c[countType])
                countMatrix+=counts
            countMatrix = countMatrix.astype(int)
            countDict[countType] = countMatrix
    return countDict

def get_stats(r, intTime=0.5, countPath=('alice', 'bob'), numTries=-1, inlcudeNullCounts=False, trim=True):
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
    countList = loop_counts(r, CHANNELSTATS, error_check_stats, intTime=intTime, 
                inlcudeNullCounts=inlcudeNullCounts, countPath=countPath, 
                numTries=numTries, trim=trim)
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

def parse_counts(msgCounts, countPath, inlcudeNullCounts, trim, error_check_function):
    goodCounts = []
    lastTimeStamp = msgCounts[-2][0]

    for j in range(1,len(msgCounts)):
        oldCount = msgCounts[j-1][1]
        newCount = msgCounts[j][1]
        # print(newCount)

        isTrim = newCount['isTrim']
        # currentIntegrationTime = newCount['integrationTime']
        # isCorrectIntegrationTime = float(
        #     defaultIntegrationTime) == float(currentIntegrationTime)
        countsValid = error_check_function(oldCount, newCount, countPath, inlcudeNullCounts)

        if countsValid:# and isCorrectIntegrationTime:
            if not trim or (trim and isTrim):
                goodCounts.append(newCount)

    return goodCounts, lastTimeStamp


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


def error_check(previousCounts, currentCounts, countPath, inlcudeNullCounts=False):
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
    currentCounts = currentCounts[countPath]
    previousCounts = previousCounts[countPath]

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

    return countsValid

def error_check_violation(previousCounts, currentCounts, countPath, inlcudeNullCounts=False):
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
    currentCounts = currentCounts[countPath]
    previousCounts = previousCounts[countPath]

    currentCountArray = np.array(currentCounts)
    previousCountArray = np.array(previousCounts)
    # print(currentCountArray, previousCountArray)

    currentSA = currentCountArray[:,1]
    currentSB = currentCountArray[:,2]
    previousSA = previousCountArray[:,1]
    previousSB = previousCountArray[:,2]

    isAliceNull = np.sum(currentSA)<1
    isBobNull = np.sum(currentSB)<1

    # Check and see if we have repeated counts. True means counts have not updated.
    doesAliceRepeat = np.sum((currentSA-previousSA)!=0)==0
    doesBobRepeat = np.sum((currentSB-previousSB)!=0)==0

    if not inlcudeNullCounts:
        # Null counts in the singles are not valid
        if isAliceNull or isBobNull:
            countsValid = False
        if doesAliceRepeat and doesBobRepeat:
            # Make sure that the counts have updated
            countsValid = False
    else:
        # Include null counts in the results. Need to catch
        # the condition where one singles rate is 0. In this
        # case we allow it to stay 0 as that isn't necessarily
        # a sign the timetagger server has frozen. If there are
        # singles counts make sure they are updating.
        if (isAliceNull==False) and doesAliceRepeat:
            countsValid = False
        if (isBobNull==False) and doesBobRepeat:
            countsValid = False

    return countsValid

def error_check_stats(previousCounts, currentCounts, countPath, inlcudeNullCounts=False):
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
    parties = countPath 
    for p in parties:
        currentArray = np.array(currentCounts[p]).astype(int)
        previousArray = np.array(previousCounts[p]).astype(int)
        if np.sum(currentArray)==np.sum(previousArray):
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

def test_stream(r,nTimes):
    global LASTTIMESTAMP
    t1 = time.time()
    LASTTIMESTAMP = '0-0'
    
    msgCounts = rh.get_data(r, CHANNELCOUNTS, LASTTIMESTAMP)
    if msgCounts is not None:
        LASTTIMESTAMP = msgCounts[-1][0]
        counts = msgCounts[-1][1]
        # print('Starting timestamp:', msgCounts[-1][0])
    i = 0
    while i<nTimes:
        msgCounts = rh.get_data(r, CHANNELCOUNTS, LASTTIMESTAMP)
        if msgCounts is not None:
            LASTTIMESTAMP = msgCounts[-1][0]
            counts = msgCounts[-1][1]
            i+=1
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

    countsArray = get_violation(r, intTime = 1., countPath='VV', numTries=100, 
        inlcudeNullCounts=True, trim=True)

    # countsArray = get_counts(r, intTime = 1., countPath='VV', numTries=100, 
    #     inlcudeNullCounts=True, trim=True)

    # countsArray = get_stats(r, intTime = 1., numTries=100, 
    #     inlcudeNullCounts=True, trim=True)
    
    print(countsArray)

    # set_integration_time(r, oldIntegrationTime, CONFIGKEY)

    # configKey = 'config:timetaggers'
    # config = rh.get_config(r, configKey)
    # print(config)

    # test_stream(r,10)
