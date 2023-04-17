from zmqhelper import Client
import json
# import redis
# import yaml


class DetectorError(Exception):
    pass


class ComparatorControlMCC():
    """
    Simple class to control the comparators via the MCC
    """

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.con = Client(ip, port)
        self.get_config()

    def get_config(self):
        configJSON = self.con.send_message('getconfig')
        if configJSON == 'null':
            raise DetectorError('Comparator is unresponsive')
        self.config = json.loads(configJSON)
        self.order = self.config['Key_Order']['Comparator']
        return self.config, self.order

    def send_config(self, cmd, vals):
        valsJson = json.dumps(vals, separators=(',', ':'))
        self.con.send_message(cmd+' '+valsJson)

    def set_comparator_values(self):
        self.get_config()
        ret = {}
        for items, vals in self.config["Comparator"].items():
            ret[items] = vals['value']
        msg = self.send_config('setcomparatorconfig', ret)
        return msg


class DetectorControlKeithley():
    """
    Simple class to control the detectors and send commands to the
    Keithley
    """

    def __init__(self, ip, port, id, el=None):  # configFile =
        # 'client.yaml'):
        self.ip = ip
        self.port = port
        self.con = Client(ip, port)
        self.get_config()
        self.id = id+'keithley'
        if el is not None:
            self.el = el
            self.st = el
        else:
            pass
            # self.st = st
        # self.create_form()
        # self.update_form()

    def get_config(self):
        configJSON = self.con.send_message('getconfig')
        if configJSON == 'null':
            raise DetectorError('Detector is unresponsive')
        self.config = json.loads(configJSON)
        self.order = self.config['Key_Order']['Comparator']
        return self.config, self.order

    def send_config(self, cmd, vals):
        valsJson = json.dumps(vals, separators=(',', ':'))
        self.con.send_message(cmd+' '+valsJson)

    def reset_detectors(self):
        msg = self.con.send_message('resetdet')
        return msg
