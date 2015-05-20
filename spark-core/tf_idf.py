# !/usr/bin/python
import sys
import xml.etree.ElementTree as ET
import re
import string
import math
from operator import add
from pyspark import SparkContext
import string
##parsing function

def parseContext(line):
    root = ET.fromstring(line)
    for element in root.iter('title'):
        title = element.text
    for element in root.iter('ns'):
        ns = element.text
    id=[]
    for element in root.iter('id'):
        id.append(element.text)
    for element in root.iter('text'):
        et = " ".join(re.findall("[a-zA-Z]+", element.text))
        words = [word for word in et.lower().split() if word.isalpha()]
    res = []
    if ns == '0':
        res = res + [((id[0],title),word) for word in words]
        print res
    return res


    ##part1


if __name__ == "__main__":
    sc = SparkContext(appName="part1")
    lines = sc.textFile(sys.argv[1], use_unicode=False)
    doc_count = lines.count()
    words = lines.flatMap(parseContext)
    words_swap = words.map(lambda (x, y): (y, x))
    wordcount = words.map(lambda s: (s, 1)).reduceByKey(lambda a, b: a + b)
    wordcount_page = words_swap.map(lambda s: (s, 1)).reduceByKey(lambda a, b: a + b)
    count_page = words.map(lambda (a, b): (a, 1)).reduceByKey(lambda a, b: a + b)
    doc_word = words_swap.distinct().map(lambda (a, b): (a, 1)).reduceByKey(lambda a, b: a + b)
    app = []
    for (((id, title), word), n) in wordcount.collect():
        word_page = words.filter(lambda x: (id, title) in x).count()
        word_all_page = words.filter(lambda x: word in x).distinct().count()
        tf_idf = (n / word_page) * math.log((doc_count / word_all_page))
        app.append((id, title), word, tf_idf)
    #get the highest value
    app = sc.parallelize(app).map(lambda s:s).sortByKey().first()
    app_result = app.map(lambda s: s).flatMap(lambda s:s)
    app_result.saveAsTextFile("output/p1_output.txt")


    sc.stop()

