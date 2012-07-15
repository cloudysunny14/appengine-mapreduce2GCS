#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2012 cloudysunny14.
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
Created on 2012/06/30

@author: harigaekiyonari
'''
from mapreduce.lib import files
from testlib import testutil 
from mapreduce import model
from googlestorage import input_readers

class GoogleStorageInputReaderTest(testutil.HandlerTestBase):
  READER_NAME = (
      "mapreduce.input_readers.CloudStorageLineInputReader")
  
  def assertDone(self, reader):
    self.assertRaises(StopIteration, reader.next)
  
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
  
  def createGSData(self, file_count, data):
    file_paths = []
    for file_number in range(file_count):
      file_path = "/gs/foo/bar%d" % file_number
      write_path = files.gs.create(file_path, mime_type='text/plain', 
                                                  acl='public-read')
      with files.open(write_path, 'a') as fp:
        fp.write(data)
      files.finalize(write_path)
      
      file_paths.append(file_path)
    return file_paths
  
  def initMockedGoogleStorageLineReader(self,
                                    initial_position,
                                    num_blocks_read,
                                    end_offset,
                                    data):
    file_paths = self.createGSData(1, data)
    r = input_readers.GoogleStorageLineInputReader(file_paths[0],
                                               initial_position,
                                               initial_position + end_offset)
    return r
    
  def assertNextEquals(self, initial_position, reader, expected_k, expected_v):
    k, v = reader.next()
    self.assertEquals(expected_k, k+initial_position)
    self.assertEquals(expected_v, v)
    
  def dtestAtStart(self):
    """If we start at position 0, read the first record."""
    blob_reader = self.initMockedGoogleStorageLineReader(
        0, 1, 100, "foo\nbar\nfoobar")
    self.assertNextEquals(0, blob_reader, 0, "foo")
    self.assertNextEquals(0, blob_reader, len("foo\n"), "bar")
    self.assertNextEquals(0, blob_reader, len("foo\nbar\n"), "foobar")
  
  def dtestOmitFirst(self):
    """If we start in the middle of a record, start with the next record."""
    blob_reader = self.initMockedGoogleStorageLineReader(
        1, 100, 100, "foo\nbar\nfoobar")
    self.assertNextEquals(0, blob_reader, len("foo\n"), "bar")
    self.assertNextEquals(0, blob_reader, len("foo\nbar\n"), "foobar")
  
  def dtestStopAtEnd(self):
    """If we pass end position, then we don't get a record past the end."""
    blob_reader = self.initMockedGoogleStorageLineReader(
        0, 1, 1, "foo\nbar")
    self.assertNextEquals(0, blob_reader, 0, "foo")
    self.assertDone(blob_reader)
  
  def testMultiByteCharactors(self):
    """If we get multibyteCharacter."""
    blob_reader = self.initMockedGoogleStorageLineReader(
        0, 1, 1, "His 1976 film, C'était un rendez-vous, purportedly features a Ferrari 275 GTB")
    self.assertNextEquals(0, blob_reader, 0, "His 1976 film, C'était un rendez-vous, purportedly features a Ferrari 275 GTB")
    self.assertDone(blob_reader)
    
  def split_input(self, file_count, shard_count):
    """Generate some files and return the reader's split of them."""
    file_paths = self.createGSData(file_count, "google coloud storage\ninput data at 0")
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.READER_NAME,
        "mapper_params": {"file_paths": file_paths},
        "mapper_shard_count": shard_count})
    readers = input_readers.GoogleStorageLineInputReader.split_input(
        mapper_spec)
    return readers
  
  def testSplitInput(self):
    """Simple case: split one file into one group."""
    readers = self.split_input(1, 1)
    self.assertEqual(1, len(readers))
    for r in readers:
      self.assertEquals({"file_path": "/gs/foo/bar0",
                        "initial_position": 0,
                        "end_position": 37L},
                      r.to_json())
    for r in readers:
      for entity in r:
        self.assertTrue(entity, "not None")

  def testSplitInputMultiFiles(self):
    """split two files into two group"""
    readers = self.split_input(2, 2)
    self.assertEqual(2, len(readers))
    blob_readers_json = [r.to_json() for r in readers]
    blob_readers_json.sort(key=lambda r: r["file_path"])
    self.assertEquals([{"file_path": "/gs/foo/bar%d" % i,
                        "initial_position": 0,
                        "end_position": 37L} for i in range(2)],
                        blob_readers_json)
  
  def testSplitInputMultiSplit(self):
    readers = self.split_input(1, 2)
    self.assertEquals(
        [{"file_path": "/gs/foo/bar0",
          "initial_position": 0,
          "end_position": 18L},
         {"file_path": "/gs/foo/bar0",
          "initial_position": 18L,
          "end_position": 37L}],
        [r.to_json() for r in readers])
    
  def testTooManyKeys(self):
    """Tests when there are too many blobkeys present as input."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.READER_NAME,
        "mapper_params": {"file_paths": ["foo"] * 1000},
        "mapper_shard_count": 2})
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.GoogleStorageLineInputReader.validate,
                      mapper_spec)

  def testNoKeys(self):
    """Tests when there are no blobkeys present as input."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.READER_NAME,
        "mapper_params": {"file_paths": []},
        "mapper_shard_count": 2})
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.GoogleStorageLineInputReader.validate,
                      mapper_spec)
    
