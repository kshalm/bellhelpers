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


def get_power(redis_db, int_time,
              COUNTTYPE='SB',
              COUNTPATH='VV',
              include_null_counts=False,
              trim=True, loop_args={}):
    if COUNTPATH == 'All':
        return get_counts(redis_db, int_time=int_time,
                          include_null_counts=include_null_counts,
                          trim=trim, loop_args=loop_args)
    else:
        counts = get_counts(redis_db, int_time=int_time,
                            count_path=COUNTPATH,
                            include_null_counts=include_null_counts,
                            trim=trim, loop_args=loop_args)[COUNTPATH]
        # print(counts)
        if COUNTTYPE == 'SA':
            val = counts[0]
        elif COUNTTYPE == 'Coinc':
            val = counts[1]
        elif COUNTTYPE == 'SB':
            val = counts[2]
        elif COUNTTYPE == 'eff_a':
            val = counts[3]
        elif COUNTTYPE == 'eff_b':
            val = counts[4]
        elif COUNTTYPE == 'All':
            val = counts
        else:
            val = counts[5]
        return val


def get_counts(r, int_time=0.2, count_path='VV',
               include_null_counts=False,
               trim=True, loop_args={}):
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
    err_check_args = {'count_path': count_path,
                      'include_null_counts': include_null_counts, 'trim': trim}

    # numTries=numTries, timeOut=timeOut, sleepTime=sleepTime)
    count_list = rh.loop_counts(
        r, CHANNELCOUNTS, error_check_counts, err_check_args,
        intTime=int_time, **loop_args)

    if count_list is None:
        return None

    count_dict = {}
    keys = count_list[0].keys()
    for count_type in keys:
        if count_path in count_type:
            sa = 0
            sb = 0
            coinc = 0
            for c in count_list:
                sa += int(c[count_type]['As'])
                sb += int(c[count_type]['Bs'])
                coinc += int(c[count_type]['C'])
            eff_a, eff_b, eff_ab = calc_efficiency(sa, sb, coinc)
            count_array = [sa, coinc, sb, eff_a, eff_b, eff_ab]
            count_dict[count_type] = count_array

    return count_dict


def get_violation(r, int_time=0.2, count_path='VV',
                  include_null_counts=False,
                  trim=True, loop_args={}):
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
    err_check_args = {'count_path': count_path,
                      'include_null_counts': include_null_counts, 'trim': trim}
    loop_args['intTime'] = int_time

    count_list = rh.loop_counts(
        r, CHANNELVIOLATION, error_check_violation,
        err_check_args, **loop_args)

    if count_list is None:
        return None

    count_dict = {}
    keys = count_list[0].keys()
    for count_type in keys:
        if count_path in count_type:
            count_matrix = np.zeros((4, 4))
            for c in count_list:
                counts = np.array(c[count_type])
                count_matrix += counts
            count_matrix = count_matrix.astype(int)
            count_dict[count_type] = count_matrix
    return count_dict


def get_stats(r, int_time=0.5, include_null_counts=False,
              extended_checks=False, extended_check_args={},
              loop_args={}, det_channels={}):
    '''
    r: Redis connection
    int_time: The amount of time to integrate for. This is rounded to the nearest integer multiple
             of 0.2s in the default configuration. So asking for 1.5s of data will actually return 1.6s.
             It depends on what the redis integration time value is set to–if that changes from 0.2s to
             say 0.3s, then the time will be rounded to the nearest integer multiple of 0.3s.
    include_null_counts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained.
    loop_args = A dictionary of kwargs to be passed to the loop_counts() function.

    Returns: a dictionary with the aggregated counts on each timetagger channel for each party.
             or returns None if no valid counts obtained.
    '''
    err_check_args = {'include_null_counts': include_null_counts,
                      'extended_checks': extended_checks,
                      'extended_check_args': extended_check_args,
                      'det_chnls': det_channels}
    loop_args['intTime'] = int_time

    count_list = rh.loop_counts(
        r, CHANNELSTATS, error_check_stats, err_check_args, **loop_args)

    if count_list is None:
        return None

    parties = ('alice', 'bob')

    count_dict = {}
    for p in parties:
        count_dict[p] = np.zeros(8).astype(int)

    for c in count_list:
        for p in parties:
            count_dict[p] += np.array(c[p]).astype(int)

    return count_dict


def calc_efficiency(sa, sb, coinc):
    '''
    Function to compute the efficiencies given singles and coincide counts.
    '''
    sig = 1
    if (sb == 0) or (sa == 0):
        return [0, 0, 0]
    else:
        eff_a = 100*coinc*1./sb
        eff_b = 100*coinc*1./sa
        eff_ab = 100*coinc*1./(np.sqrt(sa)*np.sqrt(sb))
        eff = [round(eff_a, sig), round(eff_b, sig), round(eff_ab, sig)]
        return eff


def error_check_counts(previous_counts, current_counts,
                       count_path='VV', include_null_counts=False,
                       trim=True):
    '''
    Function to make sure that the counts satisfy several conditions. These include
    the singles not being null (if that option is specified), and that the counts have
    changed from the previous record (makes sure that the timetaggers haven't frozen.)
    previous_counts: the data from the event before the one we are currently considering
    current_counts: the data from the event we are currently considering
    count_path:  Which path to count from in case there are more than one detector per station.
                'VV' is the default and returns the standard singles/coinc counts in the coinc window.
                'VV_PC' gives the counts in the specified Pockels cell windows
                'VV_Background' give the counts outside the coincidence windows.
    include_null_counts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained.
    trim: If true, only accept counts where isTrim=True.
    Returns countsValid: a boolean as to whether the counts are valid or not.
    '''
    counts_valid = True

    if trim and current_counts['isTrim'] is False:
        # Trim check. If required, check that the current value is trimmed. If not, return False
        counts_valid = False
        return counts_valid

    current_counts = current_counts[count_path]
    previous_counts = previous_counts[count_path]

    current_sa = int(current_counts['As'])
    current_sb = int(current_counts['Bs'])
    current_coinc = int(current_counts['C'])

    previous_sa = int(previous_counts['As'])
    previous_sb = int(previous_counts['Bs'])
    previousCoinc = int(previous_counts['C'])

    if not include_null_counts:
        # Null counts in the singles are not valid
        if (current_sa == 0):
            counts_valid = False
            null_exception = stExcept.NullCountsException('alice')
            raise null_exception
        if (current_sb == 0):
            counts_valid = False
            null_exception = stExcept.NullCountsException('bob')
            raise null_exception
        # Make sure that the counts have updated
    # if (current_sa == previous_sa):
    #     counts_valid = False
        # print(current_counts, previous_counts)
        # repeatException = stExcept.RepeatingTimeTaggerException('alice')
        # raise repeatException
    # if (current_sb == previous_sb):
        # counts_valid = False
        # repeatException = stExcept.RepeatingTimeTaggerException('bob')
        # raise repeatException
    else:
        # Include null counts in the results. When the counts are
        # more than 5000, there should be less than 3% probability that we
        # will have an accidental collision. So, any collision is
        # rejected, because we assume that means the timetagger is
        # frozen
        if (current_sa > 5000) and (current_sa == previous_sa):
            counts_valid = False
            repeatException = stExcept.RepeatingTimeTaggerException('alice')
            raise repeatException
        if (current_sb > 5000) and (current_sb == previous_sb):
            counts_valid = False
            repeatException = stExcept.RepeatingTimeTaggerException('bob')
            raise repeatException

    return counts_valid


def error_check_violation(previous_counts, current_counts,
                          count_path='VV',
                          include_null_counts=False, trim=True):
    '''
    Function to make sure that the violation counts satisfy several conditions. These include
    the singles not being null (if that option is specified), and that the counts have
    changed from the previous record (makes sure that the timetaggers haven't frozen.)
    previous_counts: the data from the event before the one we are currently considering
    current_counts: the data from the event we are currently considering
    count_path:  Which path to count from in case there are more than one detector per station.
                'VV' is the default and returns the standard singles/coinc counts in the coinc window.
                'VV_PC' gives the counts in the specified Pockels cell windows
                'VV_Background' give the counts outside the coincidence windows.
    include_null_counts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained.
    trim: If true, only accept counts where isTrim=True.
    Returns counts_valid: a boolean as to whether the counts are valid or not.
    '''
    counts_valid = True

    if trim and current_counts['isTrim'] is False:
        # Trim check. If required, check that
        # the current value is trimmed. If not, return False
        counts_valid = False
        return counts_valid

    # current_counts = current_counts[count_path]
    # previous_counts = previous_counts[count_path]

    current_count_array = np.array(current_counts[count_path])
    previous_count_array = np.array(previous_counts[count_path])

    current_sa = current_count_array[:, 1]
    current_sb = current_count_array[:, 2]
    previous_sa = previous_count_array[:, 1]
    previous_sb = previous_count_array[:, 2]

    is_alice_null = np.sum(current_sa) < 1
    is_bob_null = np.sum(current_sb) < 1

    # Check and see if we have repeated counts.
    # True means counts have not updated.
    does_alice_repeat = np.sum((current_sa-previous_sa) != 0) == 0
    does_bob_repeat = np.sum((current_sb-previous_sb) != 0) == 0

    if not include_null_counts:
        # Null counts in the singles are not valid
        if is_alice_null:
            counts_valid = False
            nullException = stExcept.NullCountsException('alice')
            raise nullException
        if is_bob_null:
            counts_valid = False
            nullException = stExcept.NullCountsException('bob')
            raise nullException
        # Make sure that the counts have updated
        if does_alice_repeat:
            counts_valid = False
            repeatException = stExcept.RepeatingTimeTaggerException('alice')
            raise repeatException
        if does_bob_repeat:
            counts_valid = False
            repeatException = stExcept.RepeatingTimeTaggerException('bob')
            raise repeatException
    else:
        # Include null counts in the results. Need to catch
        # the condition where one singles rate is 0. In this
        # case we allow it to stay 0 as that isn't necessarily
        # a sign the timetagger server has frozen. If there are
        # singles counts make sure they are updating.
        if (not is_alice_null) and does_alice_repeat:
            counts_valid = False
            repeatException = stExcept.RepeatingTimeTaggerException('alice')
            raise repeatException
        if (not is_bob_null) and does_bob_repeat:
            counts_valid = False
            repeatException = stExcept.RepeatingTimeTaggerException('bob')
            raise repeatException
    return counts_valid


def error_check_stats(previous_counts, current_counts,
                      det_chnls={},
                      include_null_counts=False,
                      extended_checks=False, extended_check_args={}):
    '''
    Function to make sure that the counts satisfy several conditions. These include
    the singles not being null (if that option is specified), and that the counts have
    changed from the previous record (makes sure that the timetaggers haven't frozen.)
    previous_counts: the data from the event before the one we are currently considering
    current_counts: the data from the event we are currently considering
    include_null_counts: Allow either of the singles counts to be 0 if True. If False waits until a non
                       zero singles is obtained.
    Returns counts_valid: a boolean as to whether the counts are valid or not.
    '''
    counts_valid = True
    parties = ('alice', 'bob')
    for p in parties:
        det_ch = det_chnls[p]
        current_array = np.array(current_counts[p]).astype(int).flatten()
        previous_array = np.array(previous_counts[p]).astype(int).flatten()
        if (current_array == previous_array).all():
            raise stExcept.RepeatingTimeTaggerException(p)

        elif (current_array[det_ch] <= 0 and
              previous_array[det_ch] <= 0):
            raise stExcept.DetectorNormalTimeTaggerException(p)

        if np.sum(current_array) == 0 and not include_null_counts:
            raise stExcept.NullCountsTimeTaggerException(p)

        if extended_checks:
            # do more extensive checking like making sure the syncs
            # are not drifting etc.

            sync_ch = extended_check_args['sync_ch']
            sync_drift_lim = extended_check_args['sync_drift_lim']
            sync_lower_lim = extended_check_args['sync_lower_lim']
            counts_lower_lim = extended_check_args['counts_lower_lim']
            sync_upper_lim = extended_check_args['sync_upper_lim']

            if (np.abs(previous_array[sync_ch] -
                       current_array[sync_ch]) >
                    sync_drift_lim):
                raise stExcept.LaserDriftTimeTaggerException(p)

            if (current_array[sync_ch] < sync_lower_lim
                    and current_array[det_ch] < counts_lower_lim):
                # edge case timetagger weridness
                raise stExcept.LowCountsTimeTaggerException(p)

            if (current_array[sync_ch] < sync_lower_lim):
                raise stExcept.LaserModelockTimeTaggerException(p)

            if current_array[sync_ch] > sync_upper_lim:
                raise stExcept.LaserDoublePulsingTimeTaggerException(p)

    return counts_valid


def pockels_value_settr(new_value, old_value):
    if new_value is not None:
        try:
            new_value = float(int(new_value))
        except (TypeError, ValueError):
            print("could not change pockels props. Bad argument type")
            new_value = old_value
    else:
        new_value = old_value
    return new_value


def set_get_pockels_window(r, config_key,
                           p_start=None, p_length=None):
    '''
   Function to change the integration time.
   r: Redis connection
   p_start:    The new Pockels cell start window.
               Will remain the same if None is passed.
   p_start:    The new Pockels cell window length.
               Will remain the same if None is passed.
    return value:    The previous start and length of PC window
   '''
    config = rh.get_config(r, config_key)
    p_start_old = config['pockelProp']['start']
    p_length_old = config['pockelProp']['length']
    p_start = pockels_value_settr(p_start, p_start_old)
    p_length = pockels_value_settr(p_length, p_length_old)
    config['pockelProp']['start'] = p_start
    config['pockelProp']['length'] = p_length
    rh.set_config(r, config, config_key)

    return p_start, p_length


def set_integration_time(r, int_time, config_key):
    '''
    Function to change the integration time.
    r: Redis connection
    int_time:    The new integration time
    config_key:  The redis stream containing the configuration dictionary
    Returns:    The previous integration time.
    '''
    config = rh.get_config(r, config_key)
    current_int_time = config['INT_TIME']
    if float(current_int_time) != float(int_time):
        config['INT_TIME'] = float(int_time)
        rh.set_config(r, config, config_key)

    return current_int_time


def get_integration_time(r, config_key=None):
    '''
    Function to fetch the integration time.
    r: Redis connection
    config_key:  The redis stream containing the configuration dictionary
    Returns:    The current integration time.
    '''
    if config_key is None:
        config_key = CONFIGKEY
    config = rh.get_config(r, config_key)
    current_int_time = config['INT_TIME']
    return current_int_time


def test_stream(r, n_times):
    global LASTTIMESTAMP
    t1 = time.time()
    LASTTIMESTAMP = '0-0'

    msg_counts = rh.get_data(r, CHANNELCOUNTS, LASTTIMESTAMP)
    if msg_counts is not None:
        LASTTIMESTAMP = msg_counts[-1][0]
        counts = msg_counts[-1][1]
    i = 0
    while i < n_times:
        msg_counts = rh.get_data(r, CHANNELCOUNTS, LASTTIMESTAMP)
        if msg_counts is not None:
            LASTTIMESTAMP = msg_counts[-1][0]
            counts = msg_counts[-1][1]
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
    loop_args = {}
    loop_args['numTries'] = 100
    loop_args['timeOut'] = 10

    # ut = rh.stream_last_updated(r, CHANNELCOUNTS)
    # alive, lastUpdate, lastItem= rh.is_stream_alive(r, CHANNELCOUNTS, 0.4)
    # print(alive, lastUpdate, lastItem)

    countsArray = ''
    try:
        # countsArray = get_counts(r, int_time = 1., count_path='VV',
        #     include_null_counts=False, trim=False, loop_args=loop_args)

        # countsArray = get_violation(r, int_time = 1., count_path='VV',
        #     include_null_counts=False, trim=False, loop_args=loop_args)

        countsArray = get_stats(
            r, int_time=1., include_null_counts=False, loop_args=loop_args)

    except stExcept.StreamException as e:
        print(e)
    except (stExcept.StreamFrozenException,
            stExcept.StreamTimeoutException) as e:
        print(e)

    print(countsArray)

    # set_integration_time(r, oldIntegrationTime, CONFIGKEY)

    # config_key = 'config:timetaggers'
    # config = rh.get_config(r, config_key)
    # print(config)

    # test_stream(r,10)
