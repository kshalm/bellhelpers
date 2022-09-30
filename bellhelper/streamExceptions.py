'''
Custom exceptions related to streams that can be thrown.
'''
class StreamException(Exception):
    def __init__(self, message="Stream Error"):
        self.message = message
        super().__init__(self.message)

class TimeTaggerRepeatingException(StreamException):
    def __init__(self, party, message=None):
        msg = party+"'s"
        msg = " timetagger is stuck repeating the same elements." 
        msg+= "It may need to be restarted."
        if message is None:
            message = msg
        self.party = party 
        self.message = message
        super().__init__(self.message)

class nullCountsTimeTaggerException(StreamException):
    def __init__(self, party, message=None): 
        msg = party+"'s"
        msg += " timetagger has no counts on any channel."
        if message is None:
            message = msg
        self.party = party 
        self.message = message
        super().__init__(self.message)

class nullCountsException(StreamException):
    def __init__(self, party, message=None): 
        msg = party
        msg += " has no counts detected. Detectors may need to be reset, "
        msg +="the laser is off, or some other error has occurred."
        if message is None:
            message = msg
        self.party = party 
        self.message = message
        super().__init__(self.message)

    # def __str__(self):
    #     return self.party+" "+self.message

class streamTimeoutException(StreamException):
    def __init__(self, channel='', timeElapsed='', message=None):
        msg = "Timeout retreiving from the stream"
        msg += channel+'\n'
        msg += 'Elapsed Time: '+str(timeElapsed)+'s'

        if message is None:
            message = msg
        self.channel = channel
        # self.party = party 
        self.timeElapsed = timeElapsed
        self.message = message
        super().__init__(self.message)

    # def __str__(self):
    #     return self.message+' '+self.channel+'\n'+'Elapsed Time: '+str(self.timeElapsed)+'s'

class StreamFrozenException(StreamException):
    def __init__(self, channel='', numTries=None, timeElapsed=None, message=None):
        msg = "Redis stream "
        msg += channel
        msg += " is not updating. \n"
        msg += "Attempted to connect "+str(numTries)+' times unsucessfully.\n'
        msg += "Elapsed Time: "+str(timeElapsed)+"s"
        
        if message is None:
            message = msg

        self.numTries = numTries
        self.timeElapsed = timeElapsed
        self.channel = channel
        self.message = message
        super().__init__(self.message)