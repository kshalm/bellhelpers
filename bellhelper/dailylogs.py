from logging.handlers import TimedRotatingFileHandler


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
