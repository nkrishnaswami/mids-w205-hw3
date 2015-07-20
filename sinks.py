from __future__ import print_function
import unittest
from contextlib import closing

from io import BytesIO
import gc
import os.path
import os
import json

from boto.s3.key import Key
import pymongo

class Sink(object):
    def open(self, filename):
        """Prepare a sink to receive data"""
        raise NotImplementedError
    def write(self, string):
        """Send a record to the sink"""
        raise NotImplementedError
    def flush(self):
        """Request the sync commit data"""
        raise NotImplementedError
    def close(self):
        """Indicate final record has been sent to the sink"""
        raise NotImplementedError
    def exists(self, path):
        """Indicate the specified path exists, if True"""
        return False

class StdoutSink(Sink):
    def __init__(self, key):
        self.key = key
    def open(self):
        pass
    def write(self, string):
        print(self.key, ": ", string)
    def flush(self):
        pass
    def close(self):
        pass
    def exists(self, path):
        return False

class FileSink(Sink):
    """This is a sink that wraps a Python file, except it creates missing
    subdirectories.

    """
    def __init__(self):
        self.is_open = False
        self.file=None
    def open(self, filename):
        try:
            subdir = os.path.dirname(os.path.abspath(filename))
            if(not os.path.isdir(subdir)):
                os.makedirs(subdir)
        except OSError, e:
            pass
        self.file = open(filename, 'w')
    def write(self, string):
        self.file.write(string)
    def flush(self):
        self.file.flush()
    def close(self):
        if self.file:
            self.file.close()
        self.file = None
        self.is_open = False
    def exists(self, path):
        return os.path.exists(path)

class S3Sink(Sink):
    """Sink that buffers a file in memory and writes to S3 on close
    """

    caches = {}
    def __init__(self, conn, bucket_name):
        self.is_open = False
        self.conn = conn
        self.key = None
        self._io = None
        self.bucket = self.conn.lookup(bucket_name)
        if not self.bucket:
            self.bucket = self.conn.create_bucket(bucket_name)
        if bucket_name not in S3Sink.caches:
            S3Sink.caches[bucket_name] = set(x.name for x in self.bucket.list())
            
    def open(self, filename):
        if self.is_open:
            self.close()
        self.key = Key(self.bucket, filename)
        self._io = BytesIO()
        self.is_open = True
    def write(self, string):
        self._io.write(string)
    def flush(self):
        """Since we don't have incremental write APIs, flush is a no-op
        """
    def close(self):
        if self.key and self._io:
            self.key.set_contents_from_string(self._io.getvalue())
            self._io.close()
            self._io = None
            self.key = None
        self.is_open = False
        gc.collect()
    def exists(self, path):
        return path in S3Sink.caches[self.bucket.name]

class RecordSink(Sink):
    """This is a file-like object for writing JSON arrays to file.  Each
    string (record) sent to it by write() is written to the file it
    manages, prepended with a ",\n" if not the first record.  On
    open/close, it writes the JSON "[" and "]" needed to denote an
    array.

    """
    def __init__(self, sink):
        self.is_open = False
        self.sink = sink
        self.first = None
    def open(self, filename):
        self.sink.open(filename)
        self.sink.write('[\n')
        self.first = True
        self.is_open = True
    def write(self, string):
        if not self.first:
            self.sink.write(',\n')
        else:
            self.first = False
        self.sink.write(string)
    def flush(self):
        self.sink.flush()
    def close(self):
        if self.is_open:
            self.sink.write('\n]\n')
            self.sink.close()
            self.first = None
            self.is_open = False
    def exists(self, path):
        return self.sink.exists(path)
class RollingSink(Sink):
    """This is a file-like object for writing to a "rolling" set of files,
    with a user-specified bound on the number of records to write to
    each actual file.

    """
    def __init__(self, file_format, rec_limit, sink):
        """`file_format` is a format string that contains "{0}" to indicate
        where the file count should be emitted in the file name.
        `rec_limit' is the threshold of writes after which to roll
        output files.
        `make_sink' is a routine taking no arguments that creates a
        new sink for rolling output files.

        """
        self.is_open = False
        self.sink = sink
        self.file_format = file_format
        self.rec_limit = rec_limit
        self.file_count = 0
        self.rec_count = 0
        
    def open(self):
        path = self.file_format.format(self.file_count)
        while self.sink.exists(path):
            self.file_count += 1
            path = self.file_format.format(self.file_count)
        self.sink.open(path)
        self.file_count += 1
        self.is_open = True
    def write(self, string):
        self._roll()
        self.sink.write(string)
        self.rec_count += 1
    def flush(self): 
        self.sink.flush()
        self._roll()
    def close(self):
        if self.is_open: 
            self.sink.close()
            self.is_open = False
    
    def _roll(self):
        if self.rec_count > 0 and self.rec_count >= self.rec_limit:
            self.close()
            self.rec_count = 0
        if not self.is_open:
            self.open()



class MongoDBSink(Sink):
    def __init__(self, dbname):
        self.dbclient = pymongo.MongoClient()
        self.db = self.dbclient[dbname]
        self.coll = None
        self.is_open = False
    def open(self, collname):
        """Prepare a sink to receive data"""
        if not self.is_open:
            self.coll = self.db[collname]
            self.is_open = True
    def write(self, string):
        """Send a record to the sink"""
        if self.is_open:
            self.coll.insert_one(json.loads(string))
    def flush(self):
        """Request the sync commit data"""
        pass
    def close(self):
        """Indicate final record has been sent to the sink"""
        if self.is_open:
            self.coll.create_index([('text',pymongo.TEXT)])
            self.coll = None
            self.is_open = False
    def exists(self, collname):
        """Indicate the specified path exists, if True"""
        return collname in self.db.collection_names()

class SinkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from boto.s3.connection import S3Connection
        from credentials import Credentials
        cls.creds = Credentials(os.path.expanduser('~/.aws/credentials'))
        cls.conn = S3Connection(
            cls.creds.default_aws_access_key_id,
            cls.creds.default_aws_secret_access_key)

    def check(self, f, exp):
        act = f.readline()
        self.assertEqual(act, exp)
    def test_FileSink(self):
        with closing(FileSink()) as f:
            f.open('./foo/bar/file_sink')
            self.assertTrue(os.path.exists('./foo/bar/file_sink'))
            f.write('This is a test\n')
        with open('./foo/bar/file_sink') as f:
            self.check(f, 'This is a test\n')

    def test_RecordSink(self):
        with closing(RecordSink(FileSink())) as f:
            f.open('./foo/bar/record_sink')
            f.write('"Record 1"')
            f.write('"Record 2"')
            f.write('"Record 3"')
            f.write('"Record 4"')
        with open('./foo/bar/record_sink') as f:
            self.check(f, '[\n')
            self.check(f, '"Record 1",\n')
            self.check(f, '"Record 2",\n')
            self.check(f, '"Record 3",\n')
            self.check(f, '"Record 4"\n')
            self.check(f, ']\n')
    
    def test_RollingSink(self):
        with closing(RollingSink('./foo/bar/rolling_sink.{0}', 2,
                                 RecordSink(FileSink()))) as f:
            f.open()
            f.write('"Record 1"')
            f.write('"Record 2"')
            f.write('"Record 3"')
            f.write('"Record 4"')
        with open('./foo/bar/rolling_sink.0') as f:
            self.check(f, '[\n')
            self.check(f, '"Record 1",\n')
            self.check(f, '"Record 2"\n')
            self.check(f, ']\n')
        with open('./foo/bar/rolling_sink.1') as f:
            self.check(f, '[\n')
            self.check(f, '"Record 3",\n')
            self.check(f, '"Record 4"\n')
            self.check(f, ']\n')

    def test_S3Sink(self):
        with closing(RollingSink('/foo/bar/{0}', 2, RecordSink(S3Sink(SinkTest.conn, 'nkrishna-mids205')))) as f:
            f.open()
            f.write('"Record 1"')
            f.write('"Record 2"')
            f.write('"Record 3"')
            f.write('"Record 4"')
        b = SinkTest.conn.lookup('nkrishna-mids205')
        k = Key(b, '/foo/bar/0')
        f = BytesIO()
        k.get_contents_to_file(f)
        self.assertEqual(f.getvalue(), '[\n"Record 1",\n"Record 2"\n]\n')
        k = Key(b, '/foo/bar/1')
        f = BytesIO()
        k.get_contents_to_file(f)
        self.assertEqual(f.getvalue(), '[\n"Record 3",\n"Record 4"\n]\n')

    def test_MongoDBSink(self):
        try:
            f = MongoDBSink("test_db")
            f.open("test_coll")
            self.assertTrue(f.is_open)
            f.write({'text':'fourscore and seven years ago'})
        finally:
            f.close()
        self.assertFalse(f.is_open)
        self.assertTrue(f.exists('test_coll'))
        self.assertFalse(f.exists('fake_coll'))
        for doc in f.db.test_coll.find({}):
            self.assertTrue('text' in doc)
            self.assertEqual(doc['text'], 'fourscore and seven years ago')

    @classmethod
    def tearDownClass(cls):
        os.remove('./foo/bar/file_sink')
        os.remove('./foo/bar/record_sink')
        os.remove('./foo/bar/rolling_sink.0')
        os.remove('./foo/bar/rolling_sink.1')
        os.removedirs('./foo/bar')
        
        b = SinkTest.conn.lookup('nkrishna-mids205')
        k = Key(b, '/foo/bar/0')
        k.delete()
        k = Key(b, '/foo/bar/1')
        k.delete()
        
        dbclient = pymongo.MongoClient()
        dbclient.drop_database('test_db')

def main():
    unittest.main()
if __name__ == '__main__':
    main()
