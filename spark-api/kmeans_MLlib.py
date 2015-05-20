__author__ = 'Bella'

import sys
from pyspark import SparkContext
from pyspark.mllib.feature import IDF
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt
import itertools


# P1

if __name__ == "__main__":
    # if len(sys.argv) != 1:
    #	print	>> sys.stderr, "Usage: kmeans <file>"
    #    exit(-1)

    sc = SparkContext(appName="part2")
    doc_test = sc.textFile(sys.argv[1], use_unicode=False)

    #before 1 spalce is id, after fist space before \t is title
    #text after \is content
    clean_dt = []
    for line in doc_test.collect():
        clean_dt.append(line.split('\t'))

    para = sc.parallelize(clean_dt)

    doc = para.map(lambda s: s[1])  ##get text
    splt = doc.map(lambda s: s.split())

    cn_dist = splt.flatMap(lambda s: s).distinct().count()  #distinct count

    htf1 = HashingTF()
    tf1 = htf1.transform(splt)
    idf1 = IDF().fit(tf1)
    tfidf1 = idf1.transform(tf1)

    ind_w = splt.map(lambda s: [htf1.indexOf(x) for x in s])  ##get index for each word in each doc

    tfidf1.collect()[0].indices
    tfidf1.collect()[0].values

    tfidf_v_test = []
    for i in range(doc_test.count()):
        tfidf_v_test.append(tfidf1.collect()[i].values.tolist())
    tfidf_ind_test = []
    for i in range(doc_test.count()):
        tfidf_ind_test.append(tfidf1.collect()[i].indices.tolist())

    tfidf_v = []
    for i in range(doc_test.count()):
        tfidf_v.append(tfidf1.collect()[i].values.tolist())  ##tfidf value

    tfidf_ind = []
    for i in range(doc_test.count()):
        tfidf_ind.append(tfidf1.collect()[i].indices.tolist())


    def zip(a, b):
        result = []
        list_length = len(a)
        for i in range(list_length):
            result.append((a[i], b[i]))
        return result

    tfidf_pair_test = []
    for i in range(len(tfidf_v_test)):
        tfidf_pair_test.append(dict(zip(tfidf_ind_test[i], tfidf_v_test[i])))
    tfidf_value_test = []
    for i in range(len(tfidf_pair_test)):
        tfidf_value_test.append([tfidf_pair_test[i].get(x) for x in ind_w.collect()[i]])

    tfidf_pair = []
    for i in range(len(tfidf_ind)):
        tfidf_pair.append(dict(zip(tfidf_ind[i], tfidf_v[i])))

    #tfidf_pair=dict(zip(tfidf_ind[i],tfidf_v[i])) ##create dict to put index and tfidf value pair
    tfidf_value = [[tfidf_pair[0].get(x) for x in ind_w.collect()[0]]]
    for i in range(1, 999):
        tfidf_value.append([tfidf_pair[i].get(x) for x in ind_w.collect()[i]])
    #tfidf_value=[tfidf_pair.get(x) for x in ind_w.collect()[0]]##get vaLue tfidf for each word

    a = tfidf_value_test
    b = splt.collect()
    word_ti_test = []
    for i in range(doc_test.count()):
        word_ti_test.append(dict(zip(b[i], a[i])))

    id_title_test = para.map(lambda s: s[0]).collect()  ##get title,id
    output_test = []
    for i in range(doc_test.count()):
        output_test.append(sc.parallelize(word_ti_test[i]).flatMap(lambda s: (id_title_test[i], s)).collect())

    a = tfidf_value_test
    b = splt.collect()
    word_ti_testdict = []
    for i in range(doc_test.count()):
        word_ti_testdict.append(dict(zip(b[i], a[i])))  ##appedn each dict into a list

    word_ti_dict = {}
    for i in range(doc_test.count()):
        word_ti_dict.update(zip(a[i], b[i]))  #append each list into one dict

    output_test2 = []
    for i in range(doc_test.count()):
        output_test2.append(sc.parallelize(word_ti_test[i]).map(lambda s: (id_title_test[i], s)).collect())

    output_22 = []
    for i in range(len(output_test2)):
        d = dict()
        for k, v in output_test2[i]:
            d.setdefault(k, list()).append(v)
        output_2 = list(d.items())
        output_22.append(output_2)

        #[(u'15889 Judah the Prince', [(u'in', 0.4889090092636256), (u'of', 0.41725399404249597), (u'burial', 4.829313237635384), (u'place', 5.970617508443202), (u'of', 0.41725399404249597),

    #sc.parallelize(output_test[0]).map(lambda s:s).distinct().count()
    #sc.parallelize(output_test[0]).map(lambda s:s).count()


    #P2
    #cn_dist=splt.flatMap(lambda s:s).distinct().count() #distinct count
    #htf1=HashingTF(cn_dist)
    #tf1=htf1.transform(splt)
    #idf1=IDF().fit(tf1)
    #tfidf1=idf1.transform(tf1)

    clusters = KMeans.train(tfidf1, k=50, maxIterations=10, runs=2,
                            initializationMode="random")  #run K-means clustering
    print clusters.centers  #centers of each cluster
    clusters = sc.parallelize(clusters.centers)
    #get 100 each cluster [[3.42,2.3,1.2],[5.65,3.2,1.23],....]
    sort_w = []
    for i in range(clusters.count()):
        sort_w.append(sorted(set(clusters.collect()[i]), reverse=True)[:100])
    ##try build in FAIL
    #def f(x):
    #    aa=sorted(x,reverse=True)[100:]
    #    print aa
    #sc.parallelize(clusters.centers).foreachPartition(f)

    ##try dic
    ## word_ti_testdict=[]
    ##for i in range(doc_test.count()):
    ##    word_ti_testdict.append(dict(zip(b[i],a[i]))) #create word tfidf pair dict

    test = sc.parallelize(word_ti_test).map(lambda s: s)  #all word tfidf pair para()()()
    ##[test.map(lambda s:s[0].collect()[x] for test.map(lambda s:s[1]).collect()[x] in sort_w[i]]
    ##topic word for each cluster

    test_test = test.collect()
    #[[w1,w2],[w4,w5],[w2]] target words each cluster
    word_target = [[word_ti_dict.get(x) for x in sort_w[0]]]
    for i in range(1, 49):
        word_target.append([word_ti_dict.get(x) for x in sort_w[i]])
    output1 = output_22  #key list pair ('title id', [('word',tfidf),('word2',tfidf))
    ###test.map(lambda s:s).flatMap(lambda s:s).map(lambda s:s[1]).collect()

    #for each cluster count number
    splt_temp = splt.collect()
    doc_cn = []
    for i in range(doc_test.count()):
        cn = []
        temp = splt_temp[i]
        for l in range(len(k)):
            count = 0
            for word in temp:
                if word in word_target[l]:
                    count += 1
            cn.append(count)
        doc_cn.append(cn)

    doc_cn = sc.parallelize(doc_cn)

    clus_doc = [[i for i, x in enumerate(doc_cn.collect()[0]) if x >= int(sys.argv[4])]]
    for w in range(1, 999):
        clus_doc.append([i for i, x in enumerate(doc_cn.collect()[w]) if x >= int(sys.argv[4])])

        #doc_cluster1=[]
        #for ii in range(len(doc_test.count())): #map each document that which cluster fit in,goupby doc
        #word_filt=doc_cn.map(lambda s:s[ii]).map(lambda s:s).filter(lambda s:s >=30)
        #get index which cluster  ［［1，2，3］［3，4，3］［5，343，23，3］］
        #doc_cluster1.append([i for doc_cluster[ii][i] in word_filt])

    #flagmap cluster, count doc number each cluster and filter less than 50  [(1,30),(45,20)]
    pre_fil = sc.parallelize(clus_doc).flatMap(lambda s: s).map(lambda s: (s, 1)).reduceByKey(lambda a, b: a + b)
    target_fil = pre_fil.filter(lambda s: (s[1] < 50 and s[1] > 1))

    inde = target_fil.map(lambda s: s[0])  #filtered cluster id [2,3,4,45]

    topic_tfidf = []
    for i in (inde.collect()):
        topic_tfidf.append(sorted(set(sort_w[i]), reverse=True)[:5])  #tfidf each target cluster

    word_oupt = [[word_ti_dict.get(x) for x in topic_tfidf[0]]]
    for i in range(1, len(topic_tfidf)):
        word_oupt.append([word_ti_dict.get(x) for x in topic_tfidf[i]])  #[[w1,w2],[w,w3]]

    #add title to doc_cluster
    a = clus_doc
    b = para.map(lambda s: s[0]).collect()  ## title,id rdd
    title_cluster = zip(b, a)  #[(title,[2,50,4]),(title1,[5,6,3])]
    #title,cluster pair


    ##doc title and belongs to filtered cluster [("title",[2,3]),('title2',[5,4])
    temp = []
    for i in range(doc_test.count()):
        temp.append((title_cluster[i][0], [i for i in title_cluster[i][1] if i in inde.collect()]))
    temp1 = sc.parallelize(temp).flatMapValues(lambda s: s).map(lambda (a, b): (b, a))
    #[(clusterid4,[title1,title2]),(clusterid50,[title1,title2])]
    cluster_tit = map((lambda (x, y): (x, list(y))), sorted(temp1.groupByKey().collect()))

    #change clusterid with topic_word
    word_oupt  #[[w1,w2],[w4,w5],[w2]]
    cluster_n = sorted(inde.collect())  #[1,2,3,30]
    a = word_oupt
    b = cluster_n
    topic_id = zip(b, a)  #[(1,[w1,w2]),(2,[w3,w4])]

    #[(clusterid4,[title1,title2]),(clusterid50,[title1,title2])]
    #[(1,[w1,w2]),(2,[w3,w4])]
    tit = sc.parallelize(cluster_tit)
    top = sc.parallelize(topic_id)
    fin = top.join(tit).map(lambda s: s[1]).collect()

    ##STREAMING
    topic = []
    for i in cluster_n:
        topic.append(word_target[i])

    title = sc.parallelize(cluster_tit).map(lambda s: s[1]).collect()
    pre_stream = zip(topic, title)
    stream_reorg = []
    for i in range(len(topic)):
        stream_reorg.append(list(itertools.product(topic[i], title[i])))
    stream = sc.parallelize(stream_reorg).flatMap(lambda s: s)

    pre1_stream = sc.parallelize(pre_stream)
    pre1_stream.map(lambda s: s).map(lambda s: s[0]).cartesian(pre1_stream.map(lambda s: s[1])).collect()

    prob2_output = sc.parallelize(fin)
    prob2_output = prob2_output.map(lambda s: s)
    prob2_output.saveAsTextFile(sys.argv[2])

    stream.saveAsTextFile(sys.argv[3])

    output2 = prob2_output.collect()
    for x in output2:
        print x

    sc.stop()


