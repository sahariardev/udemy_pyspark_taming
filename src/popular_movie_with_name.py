''''''
from pyspark import SparkContext,SparkConf

conf=SparkConf().setMaster("local").setAppName("Popular Movie With Name")
sc=SparkContext().getOrCreate(conf=conf)

def make_movie_dict():
    movies={}
    with open("ml-100k/u.item", encoding = "ISO-8859-1") as file:
        for line in file:
            fields=line.split("|")
            movies[int(fields[0])]=fields[1]

        return movies
'''
  Broadcast Movies dict
'''
movies=sc.broadcast(make_movie_dict())
def movies_mapper(line):
    fields=line.split()
    return (int(fields[1]),1)

movies_data=sc.textFile("ml-100k/u.data")
movies_count=movies_data.map(movies_mapper).reduceByKey(lambda x,y:x+y)
movies_count_flipped=movies_count.map(lambda x:(x[1],x[0]))
movies_count_sorted=movies_count_flipped.sortByKey()
movies_count_with_name=movies_count_sorted.map(lambda x:(x[0],movies.value[x[1]]))
movies_count_with_name.foreach(lambda x:print("'{a}' has been reviewed by {b} people".format(a=x[1],b=x[0])))
