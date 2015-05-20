__author__ = 'Bella'

import sys
from pyspark import SparkContext
from pyspark.mllib.feature import IDF
from pyspark.mllib.feature import HashingTF

if __name__ == "__main__":
    # if len(sys.argv) != 1:
    # print	>> sys.stderr, "Usage: tf-idf <file>"
    #    exit(-1)

    sc = SparkContext(appName="part1")
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
    for i in range(1, 1000):
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


    prob1_output = sc.parallelize(output_test2)
    prob1_output = prob1_output.map(lambda s: s).flatMap(lambda s: s)
    prob1_output.saveAsTextFile(sys.argv[2])

    output1 = prob1_output.collect()
    for x in output1:
        print x

    sc.stop()




