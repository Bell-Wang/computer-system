5-grams counts in the English Wikipedia database dump using Hadoop Streaming.

The input files of 3-gram counting is located under hdfs:/datasets/test/pg1342.txt and the XML chunks are located under hdfs:/datasets/en_wikipedia_dump/xml_chunks.Hadoop Streaming program is written in Python. Mapper scripts (wc_mapper.py and wiki_mapper.py) and Reducer scripts (wc_reducer.py and wiki_reducer.py) are attached along with this README.
