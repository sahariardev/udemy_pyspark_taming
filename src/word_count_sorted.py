from pyspark import SparkContext,SparkConf
import re

def splitBySpace(line):
    return re.compile(r'\W+',re.UNICODE).split(line.lower())
conf=SparkConf().setMaster("local").setAppName("Word Count")
sc=SparkContext().getOrCreate(conf=conf)
input=sc.textFile("Book.txt")
words=input.flatMap(splitBySpace)
wordCount=words.map(lambda  x:(x,1)).reduceByKey(lambda x,y:x+y)

def exchange_pos(x):
    return (x[1],x[0])

wordCountSorted=wordCount.map(exchange_pos).sortByKey()
for result in wordCountSorted.collect():
    cleanWord=result[1].encode('ascii','ignore')
    if(cleanWord):
        print("({a}, {b})".format(a=cleanWord.decode(),b=int(result[0])))
