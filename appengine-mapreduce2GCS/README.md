appengine-mapreduce2GCS
=======================

This is a fork of the Python version of the appengine-mapreduce library under development by AppEngine users.
http://code.google.com/p/appengine-mapreduce/
appengine-mapreduce2GCS is a Compornent of appengine-mapreduce.
That is can use the input file was placed in the Google Cloud Storage, 
further you can place the intermediate files, the output file to Google Cloud Storage

How To Use
------
Check out the source and let run [MapReduce Made Easy samples.](http://code.google.com/p/appengine-mapreduce/source/checkout)
And you want to use appengine-mapreduce2GCS,
As follows:
<pre><code>
from googlestorage import mapreduce_pipeline

class WordCountPipeline(base_handler.PipelineBase):
  """A Mapreduce Pipeline
  """
  def run(self):
    output = yield mapreduce_pipeline.MapreducePipeline(
        "word_count",
        "wordcount.word_count_map",
        "googlestorage.shuffler.ShufflePipeline",
        "wordcount.word_count_reduce",
        "googlestorage.input_readers.GoogleStorageLineInputReader",
        "googlestorage.output_writers.GoogleStorageOutputWriter",
        mapper_params={"file_paths": "/gs/sample_test/WordCount",
                       "gs_bucket_name": "temp_test",
                       "gs_acl": "public-read"},
        shuffler_params={"gs_bucket_name": "temp_test",
                          "mime_type": "text/plain",
                           "gs_acl": "public-read"},
        reducer_params={"gs_bucket_name": "output_test",
                         "mime_type": "text/plain",
                          "gs_acl": "public-read"},
        shards=2)
</code></pre>
Try it, Enjoy.

Get Involved
----------
Think it could be better? It's all Apache 2.0 licensed.
feel free to pull request for me.

Licence
----------
Copyright &copy; 2012 cloudysunny14
Licensed under the [Apache License, Version 2.0][Apache]
 
[Apache]: http://www.apache.org/licenses/LICENSE-2.0

Contributors
----------
cloudysunny14<cloudysunny14@gmail.com> @cloudysunny14