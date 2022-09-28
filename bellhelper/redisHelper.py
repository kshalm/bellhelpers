'''
A collection of helper functions to talk to the redis database,
fetch data from a data stream, and fetch and set dictionary items
containing configuration files.
'''
import json
import redis
# import copy
import yaml


def connect_to_redis(redisConfig):
    r = redis.Redis(host=redisConfig['ip'],
                    port=redisConfig['port'],
                    db=redisConfig['db'])
    return r


def get_last_entry(r, channel, count=1):
    '''returns a list of entries'''
    ret = r.xrevrange(channel, count=count)
    if not ret:
        return None
    ret = [decode_dict(ele[1]) for ele in ret]
    return ret


def get_last_timestamp(r, channel, count=1):
    '''returns a list of entries'''
    ret = r.xrevrange(channel, count=count)
    if not ret:
        return None
    ret = [ele[0].decode() for ele in ret]
    return ret


def set_key_to_expire(r, channel, time):
    '''sets an expiration time for a channel.
    time must be an int number of seconds.
    Otherwise it is cast'''
    r.expire(channel, int(time))


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