#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
Created on 2012/07/07

@author: harigaekiyonari
'''
import re
import unittest

from StringIO import StringIO
from mapreduce.lib import pipeline
from mapreduce.lib import files
from mapreduce.lib.files import file_service_pb
from mapreduce.lib.files import records
from google.appengine.ext import blobstore
from google.appengine.ext import db
from mapreduce import input_readers
from googlestorage import mapreduce_pipeline
from mapreduce import test_support
from testlib import testutil

def test_map(data):
  """Word Count map function."""
  k, v = data
  for s in split_into_sentences(v):
    for w in split_into_words(s.lower()):
      yield (w, "")

def split_into_sentences(s):
  """Split text into list of sentences."""
  s = re.sub(r"\s+", " ", s)
  s = re.sub(r"[\\.\\?\\!]", "\n", s)
  return s.split("\n")


def split_into_words(s):
  """Split a sentence into list of words."""
  s = re.sub(r"\W+", " ", s)
  s = re.sub(r"[_0-9]+", " ", s)
  return s.split()

def test_reduce(key, values):
  """Word count reduce function."""
  yield "%s: %d\n" % (key, len(values))


class MapreducePipelineTest(testutil.HandlerTestBase):
  """Tests for MapreducePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def createGSData(self, file_path, data):
    write_path = files.gs.create(file_path, mime_type='text/plain', 
                                                  acl='public-read')
    with files.open(write_path, 'a') as fp:
      fp.write(data)
    files.finalize(write_path)
  
  def testMapReduce(self):
    # Prepare test data
    word_count = 2

    file_path = "/gs/foo/bar"
    self.createGSData(file_path, "foo bar foo bar foo bar foo")
    # Run Mapreduce
    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".test_map",
        "googlestorage.shuffler.ShufflePipeline",
        __name__ + ".test_reduce",
        input_reader_spec="googlestorage.input_readers.GoogleStorageLineInputReader",
        output_writer_spec=
            "googlestorage.output_writers.GoogleStorageOutputWriter",
        mapper_params={"file_paths": file_path,
            "gs_bucket_name": "temp_test",
            "gs_acl": "public-read"},
        shuffler_params={"gs_bucket_name": "temp_test", 
                         "mime_type": "text/plain",
                         "gs_acl": "public-read"},
        reducer_params={"gs_bucket_name": "output_test",
                         "mime_type": "text/plain",
                         "gs_acl": "public-read",},
        shards=2)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

    # Verify reduce output.
    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    
    for output_file in p.outputs.default.value:
      with files.open(output_file, "r") as fp:
        buf = fp.read(1000000)
    filestream = StringIO(buf)
    output_data = filestream.read()
    outputList = output_data[:-1].split('\n')
    
    expected_data = ["foo: 4", "bar: 3"]
    expected_data.sort()
    outputList.sort()
    self.assertEquals(expected_data, outputList)

    # Verify that mapreduce doesn't leave intermediate files behind.
    blobInfos = blobstore.BlobInfo.all().fetch(limit=1000)
    for blobinfo in blobInfos:
      self.assertTrue(
          "Bad filename: %s" % blobinfo.filename,
          re.match("test-reduce-.*-output-\d+", blobinfo.filename))


class ReducerReaderTest(testutil.HandlerTestBase):
  """Tests for _ReducerReader."""

  def testReadPartial(self):
    input_file = files.blobstore.create()

    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        # First record is full
        proto = file_service_pb.KeyValues()
        proto.set_key("key1")
        proto.value_list().extend(["a", "b"])
        w.write(proto.Encode())
        # Second record is partial
        proto = file_service_pb.KeyValues()
        proto.set_key("key2")
        proto.value_list().extend(["a", "b"])
        proto.set_partial(True)
        w.write(proto.Encode())
        proto = file_service_pb.KeyValues()
        proto.set_key("key2")
        proto.value_list().extend(["c", "d"])
        w.write(proto.Encode())

    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    reader = mapreduce_pipeline._ReducerReader([input_file], 0)
    self.assertEquals(
        [("key1", ["a", "b"]),
         input_readers.ALLOW_CHECKPOINT,
         ("key2", ["a", "b", "c", "d"])],
        list(reader))

    # now test state serialization
    reader = mapreduce_pipeline._ReducerReader([input_file], 0)
    i = reader.__iter__()
    self.assertEquals(
        {"position": 0,
         "current_values": None,
         "current_key": None,
         "filenames": [input_file]},
        reader.to_json())

    self.assertEquals(("key1", ["a", "b"]), i.next())
    self.assertEquals(
        {"position": 19,
         "current_values": None,
         "current_key": None,
         "filenames": [input_file]},
        reader.to_json())

    self.assertEquals(input_readers.ALLOW_CHECKPOINT, i.next())
    self.assertEquals(
        {"position": 40,
         "current_values": ["a", "b"],
         "current_key": "key2",
         "filenames": [input_file]},
        reader.to_json())

    self.assertEquals(("key2", ["a", "b", "c", "d"]), i.next())
    self.assertEquals(
        {"position": 59,
         "current_values": None,
         "current_key": None,
         "filenames": [input_file]},
        reader.to_json())

    try:
      i.next()
      self.fail("Exception expected")
    except StopIteration:
      # expected
      pass

    # now do test deserialization at every moment.
    reader = mapreduce_pipeline._ReducerReader([input_file], 0)
    i = reader.__iter__()
    reader = mapreduce_pipeline._ReducerReader.from_json(reader.to_json())

    self.assertEquals(("key1", ["a", "b"]), i.next())
    reader = mapreduce_pipeline._ReducerReader.from_json(reader.to_json())

    self.assertEquals(input_readers.ALLOW_CHECKPOINT, i.next())
    reader = mapreduce_pipeline._ReducerReader.from_json(reader.to_json())

    self.assertEquals(("key2", ["a", "b", "c", "d"]), i.next())
    reader = mapreduce_pipeline._ReducerReader.from_json(reader.to_json())

    try:
      i.next()
      self.fail("Exception expected")
    except StopIteration:
      # expected
      pass


if __name__ == "__main__":
  unittest.main()
