from pyspark import SparkContext,SparkConf


def splitBySpace(line):
    return line.split(" ")


conf=SparkConf().setMaster("local").setAppName("Word Count")
sc=SparkContext().getOrCreate(conf=conf)

input=sc.textFile("Book.txt")
words=input.flatMap(splitBySpace)
result=words.countByValue()

for word, count in result.items():

    print(word)
    print(count)
    cleanWord=word.encode('ascii','ignore')
    if(cleanWord):
        print("I am here")
        print(
            "({a} , {b}) ".format(a=cleanWord.decode(),b=int(count))
        )
