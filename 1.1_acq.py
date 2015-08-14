from __future__ import print_function

from contextlib import closing
import datetime
import os

import tweepy
from boto.s3.connection import S3Connection
import pymongo

from credentials import Credentials
from collector import Collector
from facets import FilteringFacet
from matchers import RegexMatcher
from sinks import MongoDBSink

def main():
    query_terms=['#NBAFinals2015', '#Warriors']
    # We only need a single mongodb sink; FilteringFacet will just let us
    # avoid storing non-matching tweets
    sink = MongoDBSink('db_restT')
    sink.open('tweets')
    # closing ensures any files written get flushed/closed.
    with closing(
            FilteringFacet(
                RegexMatcher('(' + '|'.join(query_terms) + ')'),
                lambda key: sink)) as facet:
        creds = Credentials(os.path.expanduser('~/.tweepy'))
        auth = tweepy.AppAuthHandler(creds.consumer_key, creds.consumer_secret)
        collector = Collector(auth, facet)
        
        today = datetime.date.today()
        week = datetime.timedelta(7)

        query_ops={}
        query_ops['lang'] = 'en'
        query_ops['result_type'] = 'mixed'
        
        print("Starting search")
        collector.search(query_terms=query_terms,
                         query_ops=query_ops)
        print('All done!  Last ID processed={0}'.format(collector.last_id))
if __name__ == '__main__':
    main()
