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

"""Mapreduce shuffler implementation."""

from __future__ import with_statement


__all__ = [
    "ShufflePipeline",
    ]

from mapreduce.lib import pipeline
from mapreduce.lib.pipeline import common as pipeline_common
from mapreduce.lib import files
from mapreduce import base_handler
from mapreduce import input_readers
from mapreduce import mapper_pipeline
from mapreduce.shuffler import _HashingBlobstoreOutputWriter
from mapreduce.shuffler import _MergingReader
from mapreduce.shuffler import _SortChunksPipeline
from mapreduce.shuffler import _ShuffleServicePipeline
from googlestorage import output_writers

class _HashingGoogleStorageOutputWriter(_HashingBlobstoreOutputWriter):
  """An OutputWriter which outputs data into blobstore in key-value format.

  The output is tailored towards shuffler needs. It shards key/values using
  key hash modulo number of output files.
  """

  def __init__(self, filenames):
    """Constructor.

    Args:
      filenames: list of filenames that this writer outputs to.
    """
    _HashingBlobstoreOutputWriter.__init__(self, filenames)
  
  @classmethod
  def _get_filesystem(cls, mapper_spec):
    return "gs"


class _MergeGSPipeline(base_handler.PipelineBase):
  """Pipeline to merge sorted chunks.

  This pipeline merges together individually sorted chunks of each shard.

  Args:
    filenames: list of lists of filenames. Each list will correspond to a single
      shard. Each file in the list should have keys sorted and should contain
      records with KeyValue serialized entity.

  Returns:
    The list of filenames, where each filename is fully merged and will contain
    records with KeyValues serialized entity.
  """

  # Maximum number of values to produce in a single KeyValues proto.
  _MAX_VALUES_COUNT = 100000  # Combiners usually good for 5 orders of magnitude
  # Maximum size of values to produce in a single KeyValues proto.
  _MAX_VALUES_SIZE = 1000000

  def run(self, job_name, filenames):
    yield mapper_pipeline.MapperPipeline(
        job_name + "-shuffle-merge",
        "mapreduce.shuffler._merge_map",
        __name__ + "._MergingReader",
        output_writer_spec=
        output_writers.__name__ + ".GoogleStorageRecordsOutputWriter",
        params={
          _MergingReader.FILES_PARAM: filenames,
          "gs_bucket_name": "temp_test",
          "mime_type": "text/plain",
          "gs_acl": "public-read",
          _MergingReader.MAX_VALUES_COUNT_PARAM: self._MAX_VALUES_COUNT,
          _MergingReader.MAX_VALUES_SIZE_PARAM: self._MAX_VALUES_SIZE,
          },
        shards=len(filenames))


class _HashGSPipeline(base_handler.PipelineBase):
  """A pipeline to read mapper output and hash by key.

  Args:
    job_name: root mapreduce job name.
    filenames: filenames of mapper output. Should be of records format
      with serialized KeyValue proto.
    shards: Optional. Number of output shards to generate. Defaults
      to the number of input files.

  Returns:
    The list of filenames. Each file is of records formad with serialized
    KeyValue proto. For each proto its output file is decided based on key
    hash. Thus all equal keys would end up in the same file.
  """
  def run(self, job_name, filenames, shards=None):
    if shards is None:
      shards = len(filenames)
    yield mapper_pipeline.MapperPipeline(
            job_name + "-shuffle-hash",
            "mapreduce.shuffler._hashing_map",
            input_readers.__name__ + ".RecordsReader",
            output_writer_spec= __name__ + "._HashingGoogleStorageOutputWriter",
            params={'files': filenames, "gs_bucket_name": "temp_test", "gs_acl": "public-read"},
            shards=shards)


class ShufflePipeline(base_handler.PipelineBase):
  """A pipeline to shuffle multiple key-value files.

  Args:
    job_name: The descriptive name of the overall job.
    filenames: list of file names to sort. Files have to be of records format
      defined by Files API and contain serialized file_service_pb.KeyValue
      protocol messages.
    shards: Optional. Number of output shards to generate. Defaults
      to the number of input files.

  Returns:
    The list of filenames as string. Resulting files contain serialized
    file_service_pb.KeyValues protocol messages with all values collated
    to a single key.
  """
  def run(self, job_name, shuffler_params, filenames, shards=None):
    if files.shuffler.available():
      yield _ShuffleServicePipeline(job_name, filenames)
    else:
      hashed_files = yield _HashGSPipeline(job_name, filenames, shards=shards)
      sorted_files = yield _SortChunksPipeline(job_name, hashed_files)
      temp_files = [hashed_files, sorted_files]

      merged_files = yield _MergeGSPipeline(job_name, sorted_files)

      with pipeline.After(merged_files):
        all_temp_files = yield pipeline_common.Extend(*temp_files)
        yield mapper_pipeline._CleanupPipeline(all_temp_files)

      yield pipeline_common.Return(merged_files)
