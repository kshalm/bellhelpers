# -*- coding: utf-8 -*-
"""
Created on Tue Aug 02 16:45:00 2016

@author: Krister
"""
import time
import os.path
import numpy as np
from scipy.optimize import minimize
from scipy.optimize import basinhopping
from scipy import optimize
from bellMotors.motors.motorControlZaber import MotorControllerZaber
import readCounts as read
import logging
import datetime
import redisHelper as rh


class MirroControl():
    def __init__(self, r, ip='127.0.0.1', port = 55000, name = 'default'):
        print('autoalign', ip, port, name)
        self.r = r

        dt = datetime.date.today().strftime("%y_%m_%d")
        fnLog = dt + '_' + name +  "_Motor.log"
        fn = os.path.join('Logs',fnLog)
        self.logger = self.setup_logger(name, fn)
        # logging.basicConfig(filename=fnLog, level=logging.INFO)
        # logger.basicConfig(filename='example.log', format='%(asctime)s %(message)s')
        self.logger.info("==================================================")
        self.logger.info(name + ", " + ip)
        dtnow = datetime.datetime.now().strftime("Initializing at: %H:%M:%S")
        # print(dtnow)
        self.logger.info(dtnow)
        self.logger.info("==================================================")
        self.logger.info(" ")
        self.zb = MotorControllerZaber(ip, port= port)
        self.intTime = .8
        # Disable the Potentiometer knobs
        self.zb.potentiometer_all_enabled(False)
        # print('Set potentiometer')
        self.channels =  self.zb.channels
        self.motorInfo = {}
        self.extract_channel_path_names()
        self.BESTCOUNTS = 0
        # print('getting positions')
        self.BESTPOS = self.get_all_positions()
        # print(self.BESTPOS)
        self.name = name
        self.CONFIGKEY = 'config:timetaggers'
        # logger.info("BESTPOS:" + str(self.BESTPOS))

    def setup_logger(self, name, log_file, level=logging.INFO):
        formatter = logging.Formatter('%(asctime)s %(message)s')
        handler = logging.FileHandler(log_file)
        # handler.setFormatter(formatter)
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        return(logger)

    def get_paths(self):
        return(self.motorInfo.keys())

    def extract_channel_path_names(self):
        motorInfo = {}
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

        pathNames = set(path) # extract the unique path names
        for key in pathNames:
            motorInfo[key]={'x': {'ch':[], 'mirror':[]}, 'y': {'ch':[], 'mirror':[]}}

        for i in range(len(self.channels)):
            motorInfo[path[i]][mirrorDir[i]]['ch'].append(int(self.channels[i]))
            motorInfo[path[i]][mirrorDir[i]]['mirror'].append(mirrorNum[i])

        self.motorInfo = motorInfo

    def move_all_to_position(self, pos):
        for i in range(len(self.channels)):
            self.zb.move_absolute(self.channels[i],pos[i])

    def move_all_relative(self, pos):
        for i in range(len(self.channels)):
            self.zb.move_relative(self.channels[i],pos[i])

    def get_all_positions(self):
        pos = [-1] * len(self.channels)
        for i in range(len(self.channels)):
            #print(i, pos)
            pos[i] = self.zb.get_position(self.channels[i])
        return pos

    def get_power(self, intTime, COUNTTYPE = 'SB', COUNTPATH = 'VV'):
        # counts = read.get_power(intTime, COUNTPATH)[COUNTPATH]
        counts = read.get_counts(r, dt=0.2, countPath='VV')

        if COUNTTYPE == 'SA':
            val = counts[0]
        elif COUNTTYPE == 'SB':
            val = counts[2]
        elif COUNTTYPE == 'Coinc':
            val = counts[1]
        elif COUNTTYPE == 'effA':
            val = counts[3]
        elif COUNTTYPE == 'effB':
            val = counts[4]
        elif COUNTTYPE == 'All':
            val = counts
        else:
            val = counts[5]
        return val


    def check_bounds(self, pos, startP):
        L = 6000 * 3
        minBounds = startP - L
        maxBounds = startP + L

        # Check to see if we exceed the maximum bounds
        tooBig = np.greater(maxBounds,pos)
        tooSmall = np.greater(pos, minBounds)
        #print tooBig, tooSmall

        if (np.all(tooBig)==False or np.all(tooSmall)==False):
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
            startPos.append(self.STARTPOS[ch -1]) # correct for the zero indexing

        # pos is a relative move distance from the center point.
        #pos = np.append([0,0,0,0],pos)
        pos = np.asarray(pos)
        startPos = np.asarray(startPos)
        pos = pos * SCALE + startPos
        pos = np.round(pos)
        inBounds = self.check_bounds(pos, startPos)
        if (inBounds==False):
            val = 1E12
            return val
        for i in range(len(channels)):
            self.zb.move_absolute(channels[i], pos[i])
        # time.sleep(self.intTime)
        time.sleep(1.2)
        # time.sleep(.6)
        #######################
        counts = self.get_power(self.intTime, countType, COUNTPATH)

        if (counts > self.BESTCOUNTS):
            self.BESTPOS = self.get_all_positions()
            self.BESTCOUNTS = counts

        val = 1./(counts*1. + 1. )
        # params['q'].put(str(counts) + ', ' + str(self.BESTCOUNTS))
        # print counts, self.BESTCOUNTS
        self.log_output(str(counts) +', ' + str(self.BESTCOUNTS) +', '+ str(pos), params['q'])
        return val

    def log_output(self, msg, q = None):
        if q!= None:
            msg = str(msg)
            self.logger.info(msg)
            q.put(msg)
        # print(msg)

    def optimize_eff_scipy(self, path, countType = 'effAB', dir = 'xy', COUNTPATH = 'VV', q = None):
        # global STARTPOS, BESTPOS, BESTCOUNTS, COUNTTYPE, channels, pathVChanX, pathVChanY, pathHChanX, pathHChanY
        self.COUNTTYPE = countType
        self.BESTCOUNTS = 0.
        self.STARTPOS = self.get_all_positions()
        self.log_output("Starting Position: "+ str(self.STARTPOS), q)
        self.log_output("", q)
        xChan = []
        yChan = []

        if path.lower() == 'both':
            x0 = [0,0,0,0]
            for key in self.motorInfo:
                xChan += self.motorInfo[key]['x']['ch']
                yChan += self.motorInfo[key]['y']['ch']
        elif (dir == 'x') or (dir == 'y') or (dir =='xy'):
            x0 = [0,0]
            xChan = self.motorInfo[path]['x']['ch']
            yChan = self.motorInfo[path]['y']['ch']
        else:
            x0 =[0]
            xChan = [self.motorInfo[path]['x']['ch'][0]]
            yChan = [self.motorInfo[path]['y']['ch'][0]]

        print('Channels', xChan, yChan)


        SCALE = 4e5  # Amount to scale the step size by
        # SCALE = 1e6  # Amount to scale the step size by
        params = {'countType': countType, 'scale': SCALE, 'q': q, 'countpath': COUNTPATH}
        options = {'ftol':1.2e-3, 'maxfev':40}

        # minimizer_kwargs = {"method": "Nelder-Mead", "args": (params), "options": options}
        # niter = 10
        # stepsize = 1000/SCALE
        #
        # optionsCG = {'maxiter': 40, 'tol': 1.E-3, 'eps': stepsize}


        # Set the integration time
        oldIntTime = read.set_integration_time(self.r, self.intTime, self.CONFIGKEY)

        if dir == 'y' or dir == 'xy' or dir == 'y single' or dir == 'xy single':
            self.move_all_to_position(self.STARTPOS)
            time.sleep(3)
            params['channels'] = yChan
            params['scale'] = params['scale'] *4.
            self.log_output("Starting Y alignment, Path: " + path, q)
            self.log_output("Current, Best", q)
            resy = minimize(self.obj_func, x0, params, method = 'Nelder-Mead', options = options)
            # resy = minimize(self.obj_func, x0, params, method = 'CG', options = optionsCG)
            #resy = basinhopping(self.obj_func, x0, minimizer_kwargs = minimizer_kwargs, stepsize = stepsize )
            self.STARTPOS = self.BESTPOS
            # self.STARTPOS = self.get_all_positions()
            self.log_output("Finished Y: " + str(self.BESTPOS) + ' Optim: ' + str(self.BESTCOUNTS), q)
            self.log_output("", q)
            self.BESTCOUNTS = 0.

        if dir == 'x' or dir == 'xy' or dir == 'x single' or dir == 'xy single':
            self.move_all_to_position(self.STARTPOS)
            time.sleep(3)
            params['channels'] = xChan
            params['scale'] = params['scale']*1./3.
            self.log_output("Starting X alignment, Path: " + path, q)
            self.log_output("Current, Best", q)
            # resx = minimize(self.obj_func, x0,params, method = 'Nelder-Mead', options = options)
            resx = minimize(self.obj_func, x0, params, method = 'Nelder-Mead', options = options)
            #resx = basinhopping(optimize_x, x0, minimizer_kwargs = minimizer_kwargs, niter = niter, stepsize = stepsize)
            self.STARTPOS = self.BESTPOS
            # self.STARTPOS = self.get_all_positions()
            self.log_output("Finished X: " + str(self.BESTPOS) + ' Optim: ' + str(self.BESTCOUNTS), q)
            self.log_output("", q)
            self.BESTCOUNTS = 0.


        self.move_all_to_position(self.BESTPOS)
        # Set the integration time back to it's original value
        read.set_integration_time(self.r, oldIntTime, self.CONFIGKEY)
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
