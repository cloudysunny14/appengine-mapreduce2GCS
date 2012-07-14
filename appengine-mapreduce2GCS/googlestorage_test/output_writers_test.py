#!/usr/bin/env python
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
Created on 2012/07/04

@author: cloudysunny14
'''
from testlib import testutil

from mapreduce import model
from googlestorage import output_writers


class GoogleStorageOutputWriterTest(testutil.HandlerTestBase):
  
  FILE_WRITER_NAME = "googlestorage.output_writers.GoogleStorageOutputWriter"
  
  def create_mapper_spec(self,
                         output_writer_spec=FILE_WRITER_NAME,
                         params=None):
    params = params or {}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params,
        10,
        output_writer_spec=output_writer_spec)
    return mapper_spec
  
  def create_mapreduce_state(self, params=None):
    mapreduce_spec = model.MapreduceSpec(
        "mapreduce0",
        "mapreduce0",
        self.create_mapper_spec(params=params).to_json())
    mapreduce_state = model.MapreduceState.create_new("mapreduce0")
    mapreduce_state.mapreduce_spec = mapreduce_spec
    return mapreduce_state
  
  def testInitJob_NoSharding(self):
    mapreduce_state = self.create_mapreduce_state(
        params={"filesystem": "gs", "gs_bucket_name": "foo"})
    output_writers.GoogleStorageOutputWriter.init_job(mapreduce_state)
    self.assertTrue(mapreduce_state.writer_state)
    filenames = output_writers.GoogleStorageOutputWriter.get_filenames(mapreduce_state)
    self.assertEqual(1, len(filenames))
    self.assertTrue(filenames[0].startswith("/gs/writable:"))

  
  
  