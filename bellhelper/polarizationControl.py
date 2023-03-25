#!/usr/bin/env python
# coding: utf-8
import numpy as np
from bellMotors.motorControl import MotorController
import bellhelper.read as read
import bellhelper as bh
import logging
from bellhelper.dailylogs import MyTimedRotatingFileHandler
import datetime
from scipy.optimize import minimize
import time
import os.path
import os
import json
from functools import wraps
import threading

# Writing a new class that inherits from the TimedRotatingFileHandler
# to implement a header for every new log file. Modification of  an
# example from:
# "https://stackoverflow.com/questions/27840094/
# write-a-header-at-every-logfile-that-is-created-with-
# a-time-rotating-logger"

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
    init args:
          r - redis database/ redis connection.
          ip - ip of the motorserveer
          port - port for the motorserver
          name - optional name for the pol control object
    """

    def __init__(self, r=None, ip='127.0.0.1', port=55000,
                 name='default', log_stuff=False):
        # length of the undo and redo stack that allows for
        # simple undo and redo operations
        self.UNDO_STACK_MAX = 20
        self.r = r

        self.name = name
        self.ip = ip
        self.port = port
        self.mc = MotorController(ip, port=port)
        self.undo_stack = []
        self.redo_stack = []
        self.add_to_stack(self.update_positions(), 'undo')
        self.motor_zeros = {}
        self.init_zeros()
        self.log_stuff = log_stuff
        self.set_motor_information()
        if self.log_stuff:
            fnLog = name + "_polarization_motors"
            fn = os.path.join('motor_logs', fnLog)
            self.logger = self.setup_logger(name, fn)
            self.logger.info(self.header_updater())

    def init_zeros(self):
        for key in self.motor_list:
            self.motor_zeros[key] = 0

    def set_redis(self, redis_db):
        self.r = redis_db

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
        '''returns the updated motor positions dict and
        updates the motor_list class variable'''
        self.motor_list = self.mc.getAllPos()
        return self.motor_list

    def add_to_stack(self, positions, name):
        if name == 'undo':
            stack = self.undo_stack
        elif name == 'redo':
            stack = self.redo_stack
        else:
            raise TypeError("undefined stack name")

        if len(stack) < self.UNDO_STACK_MAX:
            stack.append(positions)
        else:
            stack.append(positions)
            # trim stack to be <= max length
            stack = stack[-self.UNDO_STACK_MAX:]

    def undo(self):
        '''returns -1 when the undo list is empty
        and a 0 on a successful undo'''
        try:
            pos = self.undo_stack.pop()
        except IndexError:
            return -1
        self.move_all_motors(pos)
        self.add_to_stack(pos, 'redo')
        return 0

    def redo(self):
        '''returns -1 when the undo list is empty
        and a 0 on a successful undo'''
        try:
            pos = self.redo_stack.pop()
        except IndexError:
            return -1
        self.move_all_motors(pos)
        self.add_to_stack(pos, 'undo')
        return 0

    def _logging_wrapper(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.log_stuff:
                start_pos = json.dumps(self.mc.getAllPos())
                ret = func(self, *args, **kwargs)
                end_pos = json.dumps(self.mc.getAllPos())
                self.logger.info("start positions : " + start_pos)
                self.logger.info("moved to : " + end_pos)
                self.logger.info("function used was : " + func.__name__
                                 + '\n\n\n')
                self.add_to_stack(self.update_positions(), 'undo')
                return ret
            else:
                # dont log, simply run the function
                return func(self, *args, **kwargs)
        return wrapper

    def set_offset(self, name, zero):
        '''set an offset for a given waveplate so that
        any waveplate operation will be refereced with that offset
        as the zero.
        Warning: Use with extreme caution!! Not a good idea for many
        reasons. Any permanent offset is best written into the
        config yaml file, and temp offsets are best handled in the
        client program'''
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
        return (logger)

    def log_output(self, *msgs):
        if self.log_stuff:
            out = ''
            for msg in msgs:
                out += str(msg)
                out += ' ,'
            self.logger.info(out[:-2])
        else:
            return
        # print(msg)

    def return_connected_motors(self):
        ret = list(self.motor_list.keys())
        ret.sort()
        return ret

    def list_waveplates(self, plates):
        if type(plates) is str:
            plates = list(self.motor_info[plates])
            plates.sort()
        move_wps = plates
        return move_wps

    def _move_motor_absolute(self, pos, motor):
        self.update_positions()
        self.mc.goto(motor, pos + self.motor_zeros[motor])
        self.update_positions()
        return

    def _move_motor_relative(self, delta, motor):
        self.update_positions()
        self.mc.forward(motor, delta)
        self.update_positions()
        return

    def thread_set(self, ang_dict):
        t = []
        mc = []
        print('debug polcontrol', ang_dict)
        for wp in ang_dict:
            ang = ang_dict[wp]
            # print(wp)
            mc.append(MotorController(self.ip, port=self.port))
            t.append(threading.Thread(target=mc[-1].goto, args=(wp, ang,)))
            t[-1].start()
        for th in t:
            th.join()
        return

    @_logging_wrapper
    def homeAll(self):
        for key in self.motor_info:
            self.mc.home(key)
        return

    @_logging_wrapper
    def move_motor_absolute(self, pos, motor):
        '''moves a single motor to some absolute position,
         offsets ARE applied if they have been previously set'''
        return self._move_motor_absolute(pos, motor)

    @_logging_wrapper
    def move_motor_relative(self, pos, motor):
        '''moves a single motor relative to current position'''
        return self._move_motor_absolute(pos, motor)

    @_logging_wrapper
    def move_wps_to_position_attr(self, pos, plates):
        '''moves a list of waveplates to some fixed position
        Inputs
            pos: a list of positions (absolute) to move to. (offsets
                                  will be applied)
            plate_attrs: a list or waveplates, or a string
                         of the form 'hwp', 'alice', etc.

        If plates is a string, the list of waveplates to be moved is
        a sorted list of all the waveplates that have that string
        as an attribute'''
        move_wps = self.list_waveplates(plates)
        if len(move_wps) != len(pos):
            raise Exception("the length of the positions list is incorrect!")
            return -1
        else:
            for i, plate in enumerate(move_wps):
                self._move_motor_absolute(pos[i], plate)
        return 0

    @_logging_wrapper
    def move_all_motors(self, motor_pos):
        '''takes in a dictionary of motor positions
        and moves all connected motors to those positions'''
        for key in motor_pos:
            self.mc.goto(key, motor_pos[key] +
                         self.motor_zeros[key])
        return 0

    @_logging_wrapper
    def optimize_wvplt_scipy(self, plates,
                             count_type='Coinc',
                             int_time=1,
                             window_type='VV',
                             method='Powell'):

        def waveplate_optimization_function(pos, params):
            mc_obj = params['mc_obj']
            waveplates = params['waveplate']
            count_type = params['count_type']
            scale = params['scale']
            start_pos = params['start_pos']
            best_counts = params['best_counts']
            window_type = params['window_type']
            int_time = params['int_time']

            # pos is a relative move distance from the center point.
            # pos = np.append([0,0,0,0],pos)

            # print(pos, startPos, SCALE, "Channels:", channels)
            pos = pos * scale + start_pos

            for i, waveplate in enumerate(waveplates):
                mc_obj.goto(waveplate, pos[i])
            # move_all_to_position(pos.tolist())
            time.sleep(.5)
            counts = read.get_power(self.r, int_time,
                                    count_type, window_type)
            # print(counts)
            # counts = counts[countIndxToOptimize]
            if (counts < best_counts):
                best_pos = []
                for waveplate in waveplates:
                    best_pos.append(float(mc_obj.getPos(waveplate)))
                params['best_pos'] = np.array(best_pos)
                params['best_counts'] = counts
            # self.log_output(counts, params['best_counts'])
            return counts

        ''' pass a list  of pol control objects and plate_attrs to choose
        a subset of waveplates to optimize
        '''
        move_wps = self.list_waveplates(plates)
        self.log_output(
            "\n The list of waveplates to be optimized is: ",
            move_wps)
        best_counts = np.inf
        start_pos = []
        for waveplate in move_wps:
            start_pos.append(float(self.mc.getPos(waveplate)))

        scale = 1  # Amount to scale the step size by
        self.log_output("\n Starting optimization at : ", start_pos,
                        '\n')
        params = {'count_type': count_type,
                  'scale': scale,
                  'start_pos': start_pos,
                  'best_counts': best_counts,
                  'best_pos': start_pos,
                  'mc_obj': self.mc,
                  'waveplate': move_wps,
                  'int_time': int_time,
                  'window_type': window_type}
        options = {'xtol': 0.2}
        # minimizer_kwargs = {"method": "Nelder-Mead",
        # "args": (params), "options": options}
        # niter = 20
        # Temp = SCALE

        x0 = np.zeros_like(start_pos)

        res = minimize(waveplate_optimization_function,
                       x0, params,
                       method=method, options=options)

        # move_all_to_position(BESTPOS)

        self.log_output("\n Finished optimization at: ",
                        params['best_pos'], 'with counts:',
                        params['best_counts'], '\n')
        for i, waveplate in enumerate(move_wps):
            self._move_motor_absolute(params['best_pos'][i],
                                      waveplate)
        return


if __name__ == '__main__':
    r =\
        bh.redisHelper.connect_to_redis({'ip': 'bellamd1.campus.nist.gov',
                                         'port': 6379, 'db': 0})
    alice = PolControl(ip='132.163.53.101', port=55000, name='alice')
    source = PolControl(ip='132.163.53.14', port=55000, name='source')
    source.set_redis(r)
    source.optimize_wvplt_scipy('alice')
