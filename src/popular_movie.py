from pyspark import SparkContext,SparkConf
conf=SparkConf().setAppName("Popular Movie ").setMaster("local")
sc=SparkContext().getOrCreate(conf=conf)

def filter_movies(line):
    data=line.split("\t")
    return (int(data[1]),1)
movies=sc.textFile("ml-100k/u.data")
movies_freq=movies.map(filter_movies).reduceByKey(lambda x,y:x+y)
flipped=movies_freq.map(lambda x:(x[1],x[0])).sortByKey(ascending=False)

results=flipped.collect()
for result in results:
    print("movie id is {a} and frequency is {b}".format(a=result[1],b=result[0]))



