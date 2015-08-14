import datetime
from signal import SIGINT
from pysigset import suspended_signals
import urllib

import tweepy

from progress.bar import Bar
from progress.spinner import Spinner
from progress.counter import Counter

from listeners import EmittingListener

class _CollectionProgress(object):
    def __init__(self, method, query_ops):
        if method == Collector._search:
            self.dates = (query_ops['since'],
                          query_ops['until'])
        elif method == Collector._stream:
            self.dates = (datetime.date.today(),
                          query_ops['until'])
        else:
            self.dates = None
        if self.dates:
            days = (self.dates[1]-self.dates[0]).days
            self._progress = Bar('Processing day: ', max=days)
        else:
            self._progress = Counter('Processing tweets: ')
    def __enter__(self):
        return self
    def __exit__(self, x, y, z):
        self.finish()
        print("\n")
    def next(self, status):
        if self.dates:
            days_elapsed = (status.created_at.date() - self.dates[0]).days
            if days_elapsed > self._progress.index:
                self._progress.next(days_elapsed - progress.index)
        else:
            self._progress.next()
    def finish(self):
        self._progress.finish()

class Collector(object):
    def __init__(self, auth, facet):
        self.last_id = None
        self.auth = auth
        self.api = tweepy.API(auth_handler=self.auth,
                              compression=True,
                              retry_errors=set((104,)),
                              retry_count=10,
                              timeout=700,
                              wait_on_rate_limit=True,
                              wait_on_rate_limit_notify=True)
        self.facet = facet
    def search(self, query_terms=[], query_ops={'count':1000}, page_limit=0):
        self._process(self._search, query_terms, query_ops, page_limit)
    def stream(self, query_terms=[], query_ops={}):
        self._process(self._stream, query_terms, query_ops, None)
    def _process(self, method, query_terms, query_ops, page_limit):
        # set up progress bar
        with _CollectionProgress(method, query_ops) as progress:
            method(query_terms, query_ops, page_limit, progress, self.facet)
    def _search(self, query_terms, query_ops, page_limit, progress, facet):
        # Are we resuming after an error
        if self.last_id:
            print('Restarting with id={0}'.format(tg.last_id))
            query_ops['max_id']=self.last_id
        # max out tweets per page
        if 'count' not in query_ops:
            query_ops['count']=1000
        
        q = '(' + ' OR '.join(query_terms) + ')'
        
        for page in tweepy.Cursor(self.api.search,
                                  q=urllib.quote_plus(q),
                                  **query_ops).pages(page_limit):
            # Block/unblock signals between pages, to allow responsive ctrl-c
            # while honoring syntactic boundaries
            with suspended_signals(SIGINT):
                for tweet in page:
                    facet.emit(tweet._json)
                    if 'id' in tweet._json:
                        last_id = tweet._json['id']
                        last_date = tweet.created_at.date()
                    progress.next(tweet)
    def _stream(self, query_terms, query_ops, page_limit, progress, facet):
        # cb for listener to tell when it's finished
        if 'until' in query_ops:
            def finished(status):
                return status.created_at.date == query_ops['until']
        elif 'count' in query_ops:
            count = 0
            def finished(status):
                count += 1
                if count == query_ops['count']:
                    return True
                return False
        else:
            def finished(status):
                return False
        # make the listener
        l = EmittingListener(facet,
                             progress,
                             finished,
                             api=self.api)
        try:
            s = tweepy.Stream(
                self.auth,
                l,
                retry_count=5,
                timeout=330)
            s.filter(track=query_terms)
        finally:
            self.last_id = l.last_id
