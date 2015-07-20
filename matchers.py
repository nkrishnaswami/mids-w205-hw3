import re

class RegexMatcher(object):
    def __init__(self, pattern):
        self.re = re.compile(pattern, re.I)
    def check(self, record):
        matches = set(key.lower() for key in self.re.findall(record))
        if matches:
            return '_'.join(matches)
        return None
