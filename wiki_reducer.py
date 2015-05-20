#!/usr/bin/python
import sys
import re
import string
import xml.etree.ElementTree as ET
from operator import add
import math
from xml.etree.ElementTree import XMLParser
if __name__ == "__main__":
    gram={}
    for line in sys.stdin:
        line = line.strip()
        if line.startswith('id') or line.startswith('title'):
            pass
        else:
            word, count = line.split('\t', 1)
        # parse the input we got from mapper.py
            try:
                count=int(count)
                gram[word]=gram.get(word,0) +count
            except ValueError:
                pass

    sorted_gram=sorted(gram.items(),key=lambda t:t[1]) ##sort

    outputlist=[]
    for word,count in sorted_gram[-5:]:
        outputlist.append([word,count])
        print '%s\t%s' % (word,count)

    outputlist.saveAsTextFile("output/wiki.txt")
