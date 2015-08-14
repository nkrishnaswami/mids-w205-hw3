from __future__ import print_function

from signal import SIGINT
from pysigset import suspended_signals

import tweepy

class EmittingListener(tweepy.StreamListener):
    def __init__(self, facet, progress, finished_fn, **kwargs):
        super(EmittingListener, self).__init__(**kwargs)
        self.facet = facet
        self.progress = progress
        self.finished_fn = finished_fn
        self.last_id = None
        self.retry_errors = set((104, 420))
    def on_timeout(self):
        print("Timeout: reconnecting")
    def on_status(self, status):
        with suspended_signals(SIGINT):
            self.last_id = status.id
            self.facet.emit(status._json)
            self.progress.next(status)
            if self.finished_fn(status):
                progress.finish()
                return False
    def on_error(self, code):
        if code in self.retry_errors:
            return
        print("Error: code={0} last_id={1}".format(code, self.last_id))
        self.progress.finish()
        self.facet.close()
        return False
    def on_exception(self, code):
        print("Exception: code={0} last_id={1}".format(code, self.last_id))
        self.progress.finish()
        self.facet.close()
        return False
