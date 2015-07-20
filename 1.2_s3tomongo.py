#!/opt/anaconda/bin/python
from __future__ import print_function
import os
import json
import pymongo
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from credentials import Credentials

def main():
    dbclient = pymongo.MongoClient()
    dbclient.db_tweets.drop_collection('tweets')
    creds = Credentials(os.path.expanduser('~/.aws/credentials'))
    conn = S3Connection(
        creds.default_aws_access_key_id,
        creds.default_aws_secret_access_key)
    bucket = conn.lookup('nkrishna-mids205-hw2')
    for key in bucket.list():
        if key.name.endswith('.jsn'):
            try:
                print("Copying tweets from", key.name)
                tweets = json.loads(key.get_contents_as_string())
                dbclient.db_tweets.tweets.insert_many(tweets)
            finally:
                pass

if __name__ == '__main__':
    main()
