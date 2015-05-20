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
    sc = SparkContext(appName="part3")
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

    filt = v.map(lambda s:s).map(similar).map(lambda s:s).filter(lambda a: a[1] <= 0.3)  ##filter threshold
    cc = filt.map(lambda a: a[0]).groupByKey()  ##(p1,(p2,p3,p4,p5))
    c_cc = filt.map(lambda a: a[0])
    ##construct graph
    g = nx.Graph(c_cc)
    ccc = nx.connected_components_subgraph(g)  # return lists of cc
    num_ccc = nx.number_connected_components(g)  # number of cc
    scc=[]
    for ele in ccc:
        scc.append([num_ccc,ele, len(cc.nodes())])

   #part3 pagerank
    #use function in reference file
    appd=[]
    for ((id,title),word,n) in wordcount.collect():
        word_page = words.filter(lambda x: (id,title) in x).count()
        appd.append((id, title), word_page)
    ccc_rdd=sc.parallelize(ccc).flatMap(lambda s:s)
    appd_rdd=sc.parallelize(appd).flatMap(lambda s:s)
    ccc_len=ccc_rdd.join(ccc_rdd,appd_rdd) ##add len to individual cc ele

    def shortest(ccc_len):
        min_v=min(ccc_len.collect()[i][2] for i in range(len(ccc_len.collect())))
        for i in range(len(ccc_len.collect())):
            if ccc_len.collect()[i][2]==min_v:
                print ccc_len.collect()[i][0]   ##get short page
    PR_data=ccc_len.flatMap(shortest(ccc_len))  ##((sp_id,title),(sp_id,title))
    new_lines = sc.textFile(sys.argv[1], 1) ##import another file
    link_sp=new_lines.filter(lambda x: x==PR_data.collect()[0])  ##filter link on short page

    ## function from reference doc
    def computeContribs(urls, rank):
        """Calculates URL contributions to the rank of other URLs."""
        num_urls = len(urls)
        for url in urls:
            yield (url, rank / num_urls)
    def parseNeighbors(urls):
        """Parses a urls pair string into urls pair."""
        parts = re.split(r'\s+', urls)
        return parts[0], parts[1]
    links = link_sp.map(lambda id: parseNeighbors(id)).distinct().groupByKey().cache()
     # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda (id, neighbors): (id, 1.0))
    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(1,sys.argv[2]):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(lambda (id, (ids, rank)): computeContribs(ids, rank))
         # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)  ##0.2 as give back para
    # Collects all URL ranks and dump them to console.

    rank_res=[]
    for (id, rank) in ranks.collect():
        rank_res.append(rank_res+ (id,rank))
    rank_result=sc.parallelize(rank_res)
    link_tran2=ranks.union(rank_result).groupByKey()  #(id,title,rank)
    sort_asc=link_tran2.sortBy(_._2).collect()  #sort
    p3_result=[]
    for id,title,rank in sort_asc[-5:]:
        p3_result.append((id,title,rank))


    p3_result = sc.parallelize(p3_result)
    p3_result = p3_result.map(lambda s: s).flatMap(lambda s:s)
    p3_result.saveAsTextFile("output/p3_output.txt")

    sc.stop()
