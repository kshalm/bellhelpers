from zmqhelper import Client
import json
# import redis
# import yaml


class DetectorControlMCC():
    """
    Simple class to control the detectors to control the MCC
    """

    def __init__(self, ip, port):
        self.con = Client(ip, port)
        self.get_config()

    def get_config(self):
        configJSON = self.con.send_message('getconfig')
        self.config = json.loads(configJSON)
        self.order = self.config['Key_Order']['Comparator']
        return self.config, self.order

    def send_config(self, cmd, vals):
        valsJson = json.dumps(vals, separators=(',', ':'))
        self.con.send_message(cmd+' '+valsJson)

    def set_values(self):
        msg = self.send_config('setcomparatorconfig', self.config)
        return msg


class DetectorControlKeithley():
    """
    Simple class to control the detectors and send commands to the
    Keithley
    """

    def __init__(self, ip, port, id, el=None):  # configFile = 'client.yaml'):
        self.con = Client(ip, port)
        self.get_config(self.con)
        self.id = id+'keithley'
        if el is not None:
            self.el = el
            self.st = el
        else:
            pass
            # self.st = st
        self.create_form()
        self.update_form()

    def get_config(self):
        configJSON = self.con.send_message('getconfig')
        self.config = json.loads(configJSON)
        self.order = self.config['Key_Order']['Comparator']
        return self.config, self.order

    def send_config(self, cmd, vals):
        valsJson = json.dumps(vals, separators=(',', ':'))
        self.con.send_message(cmd+' '+valsJson)

    def reset_detecotrs(self):
        msg = self.send_config('setdetconfig', self.config)
        return msg
