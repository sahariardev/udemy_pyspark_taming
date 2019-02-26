from pyspark import SparkConf,SparkContext
def parse_txt(line):
    tokens=line.split(" ")
    age=int(tokens[1])
    number_of_friends=int(tokens[3])
    #print("I am here")
    #print((age,(number_of_friends,1)))
    return (age,number_of_friends)

def result(x,y):
    return (x[0] + y[0],x[1] + y[1])

conf=SparkConf().setMaster("local").setAppName("AveAgeCal")
sc=SparkContext.getOrCreate(conf=conf)
lines=sc.textFile("social_network.txt")
rdd=lines.map(parse_txt)
totalByage=rdd.mapValues(lambda x:(x,1)).reduceByKey(result)
print("---------")
totalByage.foreach(lambda x:print("asdd {a}".format(a=x)))
avgByage=totalByage.mapValues(lambda  x: x[0]/x[1])
#avgByage.saveAsTextFile("age_average")


