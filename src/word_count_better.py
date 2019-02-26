from pyspark import SparkContext,SparkConf
import re

def splitBySpace(line):
    return re.compile(r'\W+',re.UNICODE).split(line.lower())
conf=SparkConf().setMaster("local").setAppName("Word Count")
sc=SparkContext().getOrCreate(conf=conf)
input=sc.textFile("Book.txt")
words=input.flatMap(splitBySpace)
result=result=words.countByValue()
for word, count in result.items():
    cleanWord=word.encode('ascii','ignore')
    if(cleanWord):
        print("({a}, {b})".format(a=cleanWord.decode(),b=int(count)))
