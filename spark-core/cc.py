#!/usr/bin/python
import sys
import xml.etree.ElementTree as ET
import re
import string
import math
from operator import add
from pyspark import SparkContext
import string
import networkx as nx
##parsing function


def parseContext(line):
    root = ET.fromstring(line)
    for element in root.iter('title'):
        title = element.text
    for element in root.iter('ns'):
        ns = element.text
    id = []
    for element in root.iter('id'):
        id.append(element.text)
    for element in root.iter('text'):
        et = " ".join(re.findall("[a-zA-Z]+", element.text))
        words = [word for word in et.lower().split() if word.isalpha()]
    res = []
    if ns == '0':
        res = res + [((id[0], title), word) for word in words]
        print res
    return res


    ##part1


if __name__ == "__main__":
    sc = SparkContext(appName="part2")
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
        app.append([(id, title, word, tf_idf)])


    ##part2 read as RDD
    v = sc.parallelized(app)
    trans = v.map(lambda (a, b): (a, list(b))).groupByKey()  ##apend word as list by id
    ##key pair similarity(e-distance)
    def similar(wf):
        fun_result = []
        list1 = {}
        list2 = {}
        for item in v[0][1]:
            fun_result.append(item[0])
            list1.setdefault(item[0],item[1])
        for item in v[1][1]:
            if item[0] not in fun_result:
                fun_result.append(item[0])
            list2.setdefault(item[0],item[1])
        result1 = []
        result2 = []
        for item in fun_result:
            if item in list1:
                result1.append(0)
            else:
                result1.append(list2[item])
            if item in list2:
                result2.append(0)
            else:
                result2.append(list1[item])
        sum = 0
        for i in range(len(result1)):
            sum = sum + (result1[i]-result2[i])**2
        result = sum**0.5
        return ((v[0][0],v[1][0]),result)

    filt = v.map(lambda s:s).map(similar).map(lambda s:s).filter(lambda a: a[1] <= sys.argv[2])  ##filter threshold
    cc = filt.map(lambda a: a[0]).groupByKey()  ##(p1,(p2,p3,p4,p5))
    c_cc = filt.map(lambda a: a[0])
    ##construct graph
    g = nx.Graph(c_cc)
    ccc = nx.connected_components_subgraph(g)  # return lists of cc
    num_ccc = nx.number_connected_components(g)  # number of cc
    scc=[]
    for ele in ccc:
        scc.append([num_ccc,ele, len(cc.nodes())])

    scc = sc.parallelize(scc)
    scc_result = scc.map(lambda s: s).flatMap(lambda s:s)
    scc_result.saveAsTextFile("output/p2_output.txt")

    sc.stop()
