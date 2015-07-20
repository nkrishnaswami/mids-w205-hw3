from __future__ import print_function
import json

class Facet(object):
    def emit(self, tweet):
        """Send structured tweet contents to a facet for potential output"""
        raise NotImplementedError
    def close(self):
        """Indicate final record has been sent to the facet"""
        raise NotImplementedError
    
class FilteringFacet(Facet):
    def __init__(self, matcher, make_sink):
        self.sinks = {}
        self.matcher = matcher
        self.make_sink = make_sink
    def emit(self, tweet):
        key = self.matcher.check(tweet['text'])
        if key:
            key = key.lower()
            if not key in self.sinks:
                print("Making sink for key: {0}".format(key))
                sink = self.make_sink(key)
                self.sinks[key] = sink
            else:
                sink = self.sinks[key]                
            sink.write(json.dumps(tweet))
            return True
        return False
    def close(self):
        for (key,sink) in self.sinks.iteritems():
            print("Closing sink for key {0}".format(key));
            sink.close()
        self.sinks = {}

