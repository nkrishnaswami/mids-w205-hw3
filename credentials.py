from ConfigParser import RawConfigParser

class Credentials(object):
    def __init__(self, filename):
        with open(filename) as cfg:    
            cp = RawConfigParser()
            cp.readfp(cfg)
            for sect in cp.sections():
                for opt in cp.options(sect):
                    self.__dict__[sect + '_' + opt] = cp.get(sect, opt)
