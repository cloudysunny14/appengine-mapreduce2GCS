#!/usr/bin/env python
#
# Copyright 2012 cloudysunny14
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

from mapreduce import output_writers
'''
Created on 2012/06/27

@author: cloudysunny14
'''
class GoogleStorageOutputWriter(output_writers.FileOutputWriter,
                                 output_writers.BlobstoreOutputWriterBase):
  """A base class of OutputWriter which outputs data into GoogleStorage."""
  @classmethod
  def _get_filesystem(cls, mapper_spec):
    return "gs"
  
class KeyValueGoogleStorageOutputWriter(output_writers.KeyValueFileOutputWriter,
                                    GoogleStorageOutputWriter):
  """Output writer for KeyValue records files in GoogleStorage."""
  @classmethod
  def _get_filesystem(cls, mapper_spec):
    return "gs"
  
class GoogleStorageRecordsOutputWriter(output_writers.FileRecordsOutputWriter,
                                    GoogleStorageOutputWriter):
  """An OutputWriter which outputs data into records format."""
  @classmethod
  def _get_filesystem(cls, mapper_spec):
    return "gs"
