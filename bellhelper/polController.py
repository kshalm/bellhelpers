#!/usr/bin/env python
# coding: utf-8
import numpy as np
from bellMotors.motorControl import MotorController
import bellhelper.read as read
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
from scipy.optimize import minimize
import time
import os.path
import os
import json
import yaml

# Writing a new class that inherits from the TimedRotatingFileHandler
# to implement a header for every new log file. Modification of  an
# example from:
# "https://stackoverflow.com/questions/27840094/
# write-a-header-at-every-logfile-that-is-created-with-
# a-time-rotating-logger"


class MyTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, logfile, when, interval,
                 backupCount, header_updater, logger):
        super(MyTimedRotatingFileHandler, self).__init__(logfile,
                                                         when,
                                                         interval,
                                                         backupCount)
        self._header_updater = header_updater
        self._log = logger

    def doRollover(self):
        super(MyTimedRotatingFileHandler, self).doRollover()
        if self._header_updater is not None:
            self._log.info(self._header_updater())


'''
# create time-rotating log handler
logHandler = MyTimedRotatingFileHandler(logfile, when='midnight')
form = '%(asctime)s %(name)s %(levelname)s: %(message)s'
logFormatter = logging.Formatter(form)
logHandler.setFormatter(logFormatter)

# create logger
log = logging.getLogger('MyLogger')
logHandler.configureHeaderWriter('test-header', log)
log.addHandler(logHandler)
log.setLevel(logging.INFO)
'''


class PolControl():
    """Class to connect to various bridge and source motors
and move them while logging their positions.
    """

    def __init__(self, r=None, ip='127.0.0.1', port=55000,
                 name='default'):

        self.r = r
        fnLog = name + "_polarization_motors"
        fn = os.path.join('motor_logs', fnLog)
        self.logger = self.setup_logger(name, fn)
        self.name = name
        self.ip = ip
        self.mc = MotorController(ip, port=port)
        self.update_positions()
        self.motor_zeros = {}
        self.init_zeros()
        self.set_motor_information()
        self.logger.info(self.header_updater())

    def set_motor_information(self):
        motor_info = {}
        for key in self.motor_list:
            parts = key.lower().split('_')
            for part in parts:
                if part in motor_info:
                    motor_info[part].add(key)
                else:
                    # add a new key
                    motor_info[part] = set([key])
        self.motor_info = motor_info
        return

    def update_positions(self):
        self.motor_list = self.mc.getAllPos()
        return self.motor_list

    def init_zeros(self):
        for key in self.motor_list:
            self.motor_zeros[key] = 0

    def move_motor_absolute(self, pos, motor):
        '''moves a single motor'''
        self.update_positions()
        self.logger.info("motor: " + motor +
                         " started at " + self.mc.getPos(motor))
        self.mc.goto(motor, pos + self.motor_zeros[motor])
        self.logger.info("motor: " + motor +
                         " moved to " + self.mc.getPos(motor))
        self.update_positions()
        return

    def move_motor_relative(self, delta, motor):
        '''moves a single motor'''
        self.update_positions()
        self.logger.info("motor: " + motor +
                         " started at " + self.mc.getPos(motor))
        self.mc.forward(motor, delta)
        self.logger.info("motor: " + motor +
                         " moved to " + self.mc.getPos(motor))
        self.update_positions()
        return

    def set_offset(self, name, zero):
        '''set an offset for a given waveplate so that
        any waveplate operation will be refereced with that offset
        as the zero.
        Warning: only use this as an ad-hoc!! Any permanent offset
        is best written into the config yaml file'''
        self.motor_zeros[name] = zero

    def header_updater(self):
        dtnow = datetime.datetime.now().strftime("Initializing at: %H:%M:%S")
        head_str = ('\n' +
                    "==================================================")
        head_str += ('\n' + "Connected to " +
                     self.name + ", " + self.ip)
        head_str += '\n' + dtnow
        head_str += ('\n' +
                     "=================================================="
                     + '\n' + 'yaml starts' + '\n' + '-----' + '\n')

        tail_str = ('\n' + '-----' + '\n' + 'yaml ends' + '\n' +
                    "==================================================="
                    + '\n')
        return head_str + self.mc.getYaml() + tail_str

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

    def homeAll(self):
        for key in self.motor_info:
            self.mc.home(key)
        return

    def move_wps_to_position_attr(self, pos, plate_attrs,
                                  operation='intersect'):
        '''moves a list of waveplates to some fixed position
        Inputs
            pos: a list of positions (absolute) to move to. offsets
                                  will be applied
            plate_attrs: a list of strings of the form 'hwp', 'alice',
                                  etc.
            operation: intersection or union. Constructs either a
                       union or intersection of waveplates
                       with given plate_attrs in the name
        After the waveplate list is constructed via intersection
        or union, it is sorted to give a consistent ordering.
        The sorted list is then moved according to the pos array. i.e.,
        sorted_plate_name[0]->pos[0], etc.
        Thows a KeyError and aborts without doing anything if the
        sorted_plate_name and pos list are of different sizes'''
        all_wp_sets = [self.motor_info[ele] for ele in plate_attrs]
        if len(plate_attrs) == 1:
            move_wps = self.motor_info[plate_attrs[0]]
        elif len(plate_attrs) >= 2:
            if operation == 'intersect':
                move_wps = all_wp_sets[0].intersect(*all_wp_sets[1:])
            elif operation == 'union':
                move_wps = all_wp_sets[0].union(*all_wp_sets[1:])
            else:
                raise Exception("operation must be union or intersection!")
                return -1
        else:
            raise Exception("plate_attrs must be a list of length atleast 1")
        move_wps = list(move_wps)
        move_wps.sort()
        if len(move_wps) != len(pos):
            raise Exception("the length of the positions list is incorrect!")
            return -1
        else:
            for i, plate in enumerate(move_wps):
                self.move_motor_absolute(pos[i], plate)
        return 0

    def return_connected_motors(self):
        ret = list(self.motor_list.keys())
        ret.sort()
        return ret

    def move_all_to_position(self, pos):
        motors = list(self.motor_list.keys())
        motors.sort()

        for i, motor in enumerate(motors):
            self.move_motor_absolute(pos[i], motor)
        return
