# !/usr/bin/python
import sys
import re
import string
import xml.etree.ElementTree as ET
from operator import add
import math
from xml.etree.ElementTree import XMLParser

if __name__ == "__main__":
    con = sys.stdin.read()

    parser = XMLParser()
    parser.feed(b'<root>')
    parser.feed(con)
    parser.feed(b'</root>')
    root = parser.close()

    for subtree in root:
        for subtree in subtree:
            for element in subtree.iter('title'):
                title = element.text
                print element.tag, title.encode('ascii', 'ignore')
            for element in subtree.iter('id'):
                print element.tag, element.text
            for element in subtree.iter('text'):
                txt = element.text
                words = " ".join(re.findall("[a-zA-Z]+", txt)).lower().split()
                for word in words:
                    for i in range(2, len(words) - 1):
                        if i > 4:
                            word = words[i - 4] + "," + words[i - 3] + "," + words[i - 2] + "," + words[i - 1] + words[
                                i]
                            print '%s\t%s' % (word, 1)


