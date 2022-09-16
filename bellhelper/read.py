import redisHelper as rh 
import time
import numpy as np

CHANNELCOUNTS = 'monitor:counts'
LASTTIMESTAMP = '0-0'
CONFIGKEY = 'config:timetaggers'

def get_counts(r, countPath='VV', numTries=-1, inlcudeNullCounts=False, trim=True):
    '''
    r: Redis connection
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
    global LASTTIMESTAMP
    if numTries == 0:
        numTries = 1

    # if inlcudeNullCounts:
    #     trim = False

    msgCounts = rh.get_data(r,CHANNELCOUNTS, LASTTIMESTAMP)
    if msgCounts is not None:
        LASTTIMESTAMP = msgCounts[-2][0]
    # LASTTIMESTAMP = '0-0'
    
    intTime = get_integration_time(r, CONFIGKEY)
    # time.sleep(intTime)
    cont = True
    
    i = 0
    while cont :
        time.sleep(.1) 
        msgCounts = rh.get_data(r,CHANNELCOUNTS, LASTTIMESTAMP, count=2)
        if msgCounts is not None:
            newTimeStamp = msgCounts[-1][0]
            counts = msgCounts[-1][1]
        else:
            continue
        isTrim = counts['isTrim']
        countIntegrationTime = counts['integrationTime']
        isCorrectIntegrationTime = float(intTime) == float(countIntegrationTime)
        countsValid = error_check(msgCounts, countPath, inlcudeNullCounts)
        if countsValid and isCorrectIntegrationTime:
            if not trim or (trim and isTrim): 
                LASTTIMESTAMP = newTimeStamp
                cont = False
                break # end the loop

        if (i>=numTries-1) and (numTries>0):
            cont = False
            return None # No valid answer
        i +=1

    countVals = counts[countPath]
    sA = int(countVals['As'])
    sB = int(countVals['Bs'])
    coinc = int(countVals['C'])
    effA, effB, effAB = calc_efficiency(sA, sB, coinc)
    countArray = [sA, coinc, sB, effA, effB, effAB]

    return countArray


def calc_efficiency(sA, sB, coinc):
    '''
    Function to compute the efficiencyies given singles and coincide counts.
    '''
    sig = 1
    if (sB==0) or (sA==0):
        return [0,0,0]
    else: 
        effA = 100*coinc*1./sB 
        effB = 100*coinc*1./sA 
        effAB = 100*coinc*1./(np.sqrt(sA)*np.sqrt(sB))
        eff = [round(effA,sig), round(effB,sig), round(effAB,sig)] 
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
    if len(msgCounts)>1:
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
            if (currentSA==0) or (currentSB==0):
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

        # if not inlcudeNullCounts:
        #     if (currentSA==0) or (currentSB==0):
        #         countsValid = False 
        #         print('0 detected', countsValid)

        # # print(currentSA, previousSA, currentSB, previousSB)
        # if (currentSA == previousSA) or (currentSB == previousSB):
        #     countsValid = False
    else:
        countsValid = False
    # print('countsValid', countsValid)

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
    countsArray = get_counts(r, countPath='VV', numTries=10, inlcudeNullCounts=True, trim=False)
    print(countsArray)
    # set_integration_time(r, oldIntegrationTime, CONFIGKEY)

    # configKey = 'config:timetaggers'
    # config = rh.get_config(r, configKey)
    # print(config)




