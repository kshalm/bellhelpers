'''
A collection of helper functions to talk to the redis database,
fetch data from a data stream, and fetch and set dictionary items
containing configuration files.
'''
import json
import redis
# import copy
import yaml
import math
import time
try:
    import bellhelper.streamExceptions as stExcept
except Exception as e:
    import streamExceptions as stExcept


def connect_to_redis(redisConfig):
    r = redis.Redis(host=redisConfig['ip'],
                    port=redisConfig['port'],
                    db=redisConfig['db'])
    return r

def get_last_entry(r, channel, count=1):
    '''returns a list of entries'''
    msg = r.xrevrange(channel, count=count)
    if len(msg) == 0:
        return None
    msgDecode = [(ele[0].decode(), decode_dict(ele[1])) for ele in msg]
    return msgDecode

def set_key_to_expire(r, channel, time):
    '''sets an expiration time for a channel.
    time must be an int number of seconds.
    Otherwise it is cast'''
    r.expire(channel, int(time))

def stream_last_updated(r, channel):
    msg = get_last_entry(r, channel, count=1)
    if msg is not None:
        lastStreamTimeString = msg[0][0]
    else:
        return None
    # print(lastStreamTimeString)
    lastStreamTime = int(lastStreamTimeString.split('-')[0])
    lastStreamTime = lastStreamTime*1./1000.
    redisTime = r.time() #returns (seconds, microseconds)
    redisCurrentTime = redisTime[0]+redisTime[1]*1./1000000.
    timeSinceLastUpdate = redisCurrentTime-lastStreamTime
    return timeSinceLastUpdate

def is_stream_alive(r, channel, timeOut):
    lastUpdate = stream_last_updated(r, channel)
    alive = lastUpdate <= timeOut
    return alive, lastUpdate


def get_data(r, channel, lastTimeStamp, count=None):
    stream = {}
    stream = {channel: lastTimeStamp}
    msg = r.xread(stream, count=count)
    if len(msg) == 0:
        return None
    msgDecode = decode_data(msg[0])
    return msgDecode


def send_to_redis(r, channel, data, max_len=100):
    returnDict = {}
    for key in data.keys():
        try:
            returnDict[key] = json.dumps(data[key])
        except Exception:
            for k in data[key].keys():
                returnDict[key][k] = json.dumps(data[key][k])
    msg = r.xadd(channel, returnDict, maxlen=max_len)
    return msg


def decode_data(rawdata):
    retChannel = rawdata[0]
    encodedData = rawdata[1]

    msgDecode = []
    for m in encodedData:
        timeStamp = m[0].decode()
        data = decode_dict(m[1])
        msgDecode.append((timeStamp, data))
    return msgDecode


def decode_dict(dict):
    retDict = {}
    for key in dict.keys():
        val = dict[key].decode()
        try:
            val = json.loads(val)
        except Exception:
            val = dict[key].decode()
        key = key.decode()
        retDict[key] = val
    return retDict


def set_config(r, config, configKey):
    configJSON = json.dumps(config)
    r.set(configKey, configJSON)
    return configJSON


def get_config(r, configKey):
    msg = r.get(configKey)
    config = json.loads(msg)
    return config


def load_config_from_file(fname):
    config_fp = open(fname, 'r')
    config = yaml.load(config_fp, Loader=yaml.SafeLoader)
    config_fp.close()
    return config


def write_config_to_file(config, fname='client.yaml'):
    config_fp = open(fname, 'w')
    yaml.dump(config, config_fp, default_flow_style=False)
    config_fp.close()
    return config


def set_key_value(d, key, val):
    if key in d:
        d[key] = val
        return d
    for k, v in d.items():
        if isinstance(v, dict):
            subD = set_key_value(v, key, val)
            d[k] = subD
    return d

def loop_counts(r, channel, error_function, errorArgs, intTime=0.2, numTries=-1, sleepTime=0.05, timeOut=None):
    if numTries == 0:
        numTries = 1

    msgCounts = get_last_entry(r, channel, count=2)
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
            time.sleep(sleepTime)
            msgCounts = get_data(r, channel, LASTTIMESTAMP)
            # t2 = time.time()
            # print(i, j, 'Elapsed time:', t2-t1)
            if (msgCounts is not None) and len(msgCounts)>=2:
                # print(msgCounts)
                # Need to make sure that an update has occured which requires
                # at least two new entries since the first one.
                goodCounts, LASTTIMESTAMP = parse_counts(msgCounts, error_function, errorArgs) 

                                        # inlcudeNullCounts, trim, error_function)

                if len(goodCounts)==0:
                    continue
                else: 
                    samplesAvailable = len(goodCounts)
                    samplesLeftToAdd = nSamples - j 

                    if samplesAvailable<=samplesLeftToAdd:
                        countsToAdd = goodCounts 
                    else:
                        countsToAdd = goodCounts[-samplesLeftToAdd:-1]
                    j+=len(countsToAdd)
                    countList+=countsToAdd
                    break  # end the loop
            else:
                timeElapsed = time.time() - t1
                numTriesExceeded = (i >= numTries) and (numTries > 0)
               
                if numTriesExceeded:
                    err = stExcept.StreamFrozenException(channel, numTries=numTries, 
                        timeElapsed=timeElapsed)
                    raise err

                timeOutExceeded = (timeOut is not None) and (timeElapsed>timeOut)
                if timeOutExceeded:
                    err = stExcept.streamTimeoutException(channel, timeElapsed=timeElapsed)
                    raise err
                continue

    return countList


def parse_counts(msgCounts, error_function, errorArgs):
    goodCounts = []
    lastTimeStamp = msgCounts[-2][0]

    for j in range(1,len(msgCounts)):
        oldCount = msgCounts[j-1][1]
        newCount = msgCounts[j][1]

        countsValid = error_function(oldCount, newCount, **errorArgs)

        if countsValid:
            goodCounts.append(newCount)

    return goodCounts, lastTimeStamp