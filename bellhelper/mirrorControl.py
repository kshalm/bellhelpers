# -*- coding: utf-8 -*-
"""
Created on Tue Aug 02 16:45:00 2016

@author: Krister
"""
import time
import os.path
import numpy as np
from scipy.optimize import minimize
# from scipy.optimize import basinhopping
# from scipy import optimize
from bellMotors.motorControlZaber import MotorControllerZaber
from bellhelper.dailylogs import MyTimedRotatingFileHandler
import bellhelper.read as read
import logging
import datetime
import bellhelper.redisHelper as rh

# Writing a new class that inherits from the TimedRotatingFileHandler
# to implement a header for every new log file. Modification of  an
# example from:
# "https://stackoverflow.com/questions/27840094/
# write-a-header-at-every-logfile-that-is-created-with-
# a-time-rotating-logger"


class MirrorControl():
    def __init__(self, r, ip='127.0.0.1', port=55000, name='default'):
        # print('autoalign', ip, port, name)
        self.r = r
        fnLog = name + "zaber_motor"
        fn = os.path.join('motor_logs', fnLog)
        self.logger = self.setup_logger(name, fn)
        # logging.basicConfig(filename=fnLog, level=logging.INFO)
        # logger.basicConfig(filename='example.log', format='%(asctime)s %(message)s')
        # print(dtnow)
        self.zb = MotorControllerZaber(ip, port=port)
        self.intTime = .8
        # Disable the Potentiometer knobs
        self.zb.potentiometer_all_enabled(False)
        # print('Set potentiometer')
        self.channels = self.zb.channels
        self.motor_info = {}
        self.extract_channel_path_names()
        self.BESTCOUNTS = 0
        # print('getting positions')
        self.BESTPOS = self.get_all_positions()
        # print(self.BESTPOS)
        self.name = name
        self.CONFIGKEY = 'config:timetaggers'
        # logger.info("BESTPOS:" + str(self.BESTPOS))

    # def setup_logger(self, name, log_file, level=logging.INFO):
    #     formatter = logging.Formatter('%(asctime)s %(message)s')
    #     handler = logging.FileHandler(log_file)
    #     # handler.setFormatter(formatter)
    #     logger = logging.getLogger(name)
    #     logger.setLevel(level)
    #     logger.addHandler(handler)
    #     logger.setFormatter(formatter)
    #     return(logger)

    def setup_logger(self, name, log_file, level=logging.INFO):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        formatter = logging.Formatter('%(asctime)s %(message)s')
        logger = logging.getLogger(name)
        handler = MyTimedRotatingFileHandler(log_file,
                                             'midnight', 1, -1,
                                             self.header_updater,
                                             logger)
        handler.suffix = '%d_%m_%Y.log'
        handler.setFormatter(formatter)
        logger.setLevel(level)
        logger.addHandler(handler)
        return(logger)

    def header_updater(self):
        dtnow = datetime.datetime.now().strftime("Initializing at: %H:%M:%S")
        head_str = ('\n' +
                    "==================================================")
        head_str += ('\n' + "Connected to " +
                     self.name + ", " + self.ip)
        head_str += '\n' + dtnow
        head_str += ('\n' +
                     "==================================================" +
                     '\n')

        tail_str = ('\n'
                    "==================================================="
                    + '\n')
        pos = "positions are: " + str(self.get_all_positions())
        return head_str + pos + tail_str

    def get_paths(self):
        return(self.motor_info.keys())

    def extract_channel_path_names(self):
        motor_info = {}
        path = []
        mirrorNum = []
        mirrorDir = []

        # Extract the path, dir, and mirror number for each channel
        for i in range(len(self.channels)):
            ch = self.zb.channelNames[i]
            chVal = ch.split('_')
            # print(self.channels[i], chVal)
            path.append(chVal[0])
            mirrorNum.append(chVal[1][0])
            mirrorDir.append(chVal[1][1])

        pathNames = set(path)  # extract the unique path names
        for key in pathNames:
            motor_info[key] = {'x': {'ch': [], 'mirror': []},
                               'y': {'ch': [], 'mirror': []}}

        for i in range(len(self.channels)):
            motor_info[path[i]][mirrorDir[i]]['ch'].append(
                int(self.channels[i]))
            motor_info[path[i]][mirrorDir[i]]['mirror'].append(mirrorNum[i])

        self.motor_info = motor_info

    def move_all_to_position(self, pos):
        for i in range(len(self.channels)):
            self.zb.move_absolute(self.channels[i], pos[i])

    def move_all_relative(self, pos):
        for i in range(len(self.channels)):
            self.zb.move_relative(self.channels[i], pos[i])

    def get_all_positions(self):
        pos = [-1] * len(self.channels)
        for i in range(len(self.channels)):
            # print(i, pos)
            pos[i] = self.zb.get_position(self.channels[i])
        return pos

    def check_bounds(self, pos, startP):
        L = 6000 * 3
        minBounds = startP - L
        maxBounds = startP + L

        # Check to see if we exceed the maximum bounds
        tooBig = np.greater(maxBounds, pos)
        tooSmall = np.greater(pos, minBounds)
        # print tooBig, tooSmall

        if (np.all(tooBig) is False or np.all(tooSmall) is False):
            print("Out of bounds")
            return False
        return True

    def obj_func(self, pos, params):
        channels = params['channels']
        countType = params['countType']
        COUNTPATH = params['countpath']
        SCALE = params['scale']
        startPos = []
        for ch in channels:
            # correct for the zero indexing
            startPos.append(self.STARTPOS[ch - 1])

        # pos is a relative move distance from the center point.
        # pos = np.append([0,0,0,0],pos)
        pos = np.asarray(pos)
        startPos = np.asarray(startPos)
        pos = pos * SCALE + startPos
        pos = np.round(pos)
        inBounds = self.check_bounds(pos, startPos)
        if (not inBounds):
            val = 1E12
            return val
        for i in range(len(channels)):
            self.zb.move_absolute(channels[i], pos[i])
        # time.sleep(self.intTime)
        time.sleep(1.2)
        # time.sleep(.6)
        #######################
        counts = read.get_power(self.r, self.intTime,
                                countType, COUNTPATH)

        if (counts > self.BESTCOUNTS):
            self.BESTPOS = self.get_all_positions()
            self.BESTCOUNTS = counts

        val = 1./(counts*1. + 1.)
        # params['q'].put(str(counts) + ', ' + str(self.BESTCOUNTS))
        # print counts, self.BESTCOUNTS
        self.log_output(str(counts) + ', ' +
                        str(self.BESTCOUNTS) + ', ' + str(pos), params['q'])
        return val

    def log_output(self, msg, q=None):
        msg = str(msg)
        self.logger.info(msg)
        if q is not None:
            q.put(msg)
        # print(msg)

    def optimize_eff_scipy(self, path, countType='effAB', dir='xy',
                           COUNTPATH='VV', q=None):
        # global STARTPOS, BESTPOS, BESTCOUNTS, COUNTTYPE, channels, pathVChanX, pathVChanY, pathHChanX, pathHChanY
        self.COUNTTYPE = countType
        self.BESTCOUNTS = 0.
        self.STARTPOS = self.get_all_positions()
        self.log_output("Starting Position: " + str(self.STARTPOS), q)
        self.log_output("", q)
        xChan = []
        yChan = []

        if path.lower() == 'both':
            x0 = [0, 0, 0, 0]
            for key in self.motor_info:
                xChan += self.motor_info[key]['x']['ch']
                yChan += self.motor_info[key]['y']['ch']
        elif (dir == 'x') or (dir == 'y') or (dir == 'xy'):
            x0 = [0, 0]
            xChan = self.motor_info[path]['x']['ch']
            yChan = self.motor_info[path]['y']['ch']
        else:
            x0 = [0]
            xChan = [self.motor_info[path]['x']['ch'][0]]
            yChan = [self.motor_info[path]['y']['ch'][0]]

        print('Channels', xChan, yChan)

        SCALE = 4e5  # Amount to scale the step size by
        # SCALE = 1e6  # Amount to scale the step size by
        params = {'countType': countType, 'scale': SCALE,
                  'q': q, 'countpath': COUNTPATH}
        options = {'ftol': 1.2e-3, 'maxfev': 40}

        # minimizer_kwargs = {"method": "Nelder-Mead", "args": (params), "options": options}
        # niter = 10
        # stepsize = 1000/SCALE
        #
        # optionsCG = {'maxiter': 40, 'tol': 1.E-3, 'eps': stepsize}

        # Set the integration time
        # oldIntTime = read.set_integration_time(
        #    self.r, self.intTime, self.CONFIGKEY)

        if (dir == 'y' or dir == 'xy' or dir == 'y single'
                or dir == 'xy single'):
            self.move_all_to_position(self.STARTPOS)
            time.sleep(3)
            params['channels'] = yChan
            params['scale'] = params['scale'] * 4.
            self.log_output("Starting Y alignment, Path: " + path, q)
            self.log_output("Current, Best", q)
            resy = minimize(self.obj_func, x0, params,
                            method='Nelder-Mead', options=options)
            # resy = minimize(self.obj_func, x0, params, method = 'CG', options = optionsCG)
            # resy = basinhopping(self.obj_func, x0, minimizer_kwargs = minimizer_kwargs, stepsize = stepsize )
            self.STARTPOS = self.BESTPOS
            # self.STARTPOS = self.get_all_positions()
            self.log_output("Finished Y: " + str(self.BESTPOS) +
                            ' Optim: ' + str(self.BESTCOUNTS), q)
            self.log_output("", q)
            self.BESTCOUNTS = 0.

        if (dir == 'x' or dir == 'xy' or dir == 'x single'
                or dir == 'xy single'):
            self.move_all_to_position(self.STARTPOS)
            time.sleep(3)
            params['channels'] = xChan
            params['scale'] = params['scale']*1./3.
            self.log_output("Starting X alignment, Path: " + path, q)
            self.log_output("Current, Best", q)
            # resx = minimize(self.obj_func, x0,params, method = 'Nelder-Mead', options = options)
            resx = minimize(self.obj_func, x0, params,
                            method='Nelder-Mead', options=options)
            # resx = basinhopping(optimize_x, x0, minimizer_kwargs = minimizer_kwargs, niter = niter, stepsize = stepsize)
            self.STARTPOS = self.BESTPOS
            # self.STARTPOS = self.get_all_positions()
            self.log_output("Finished X: " + str(self.BESTPOS) +
                            ' Optim: ' + str(self.BESTCOUNTS), q)
            self.log_output("", q)
            self.BESTCOUNTS = 0.

        self.move_all_to_position(self.BESTPOS)
        # Set the integration time back to it's original value
        # read.set_integration_time(self.r, oldIntTime, self.CONFIGKEY)
        if q is not None:
            q.put('END')

# IPZaberSource = '132.163.53.83'
# IPZaberBob = '132.163.53.126'
# IPZaberAlice = '132.163.53.86'
#
# zSource = autoAlign(IPZaberSource)
# zBob = autoAlign(IPZaberBob)
# zAlice = autoAlign(IPZaberAlice)
#
# def align_all():
#     print("Aligning Source Lab")
#     zSource.optimize_eff_scipy('Both', 'effAB', 'xy')
#     print("Aligning Alice's Lab")
#     zAlice.optimize_eff_scipy('VPath', 'effA', 'xy')
#     print("Aligning Bob's Lab")
#     zBob.optimize_eff_scipy('VPath', 'effB', 'xy')


if __name__ == '__main__':
    r =\
        rh.connect_to_redis({'ip': 'bellamd1.campus.nist.gov',
                             'port': 6379, 'db': 0})
    alice = MirrorControl(r, ip='132.163.53.101', port=55000, name='alice')
    source = MirrorControl(r, ip='132.163.53.218', port=55000, name='source')
    source.optimize_eff_scipy('Both', 'effAB', 'xy')
