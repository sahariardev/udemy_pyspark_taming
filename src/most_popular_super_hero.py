from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("local").setAppName("Most Popular Superhero")
sc=SparkContext().getOrCreate(conf=conf)

def names_mapper(line):
    data=line.split("\"")
    return (int(data[0]),data[1])

super_hero_names_data=sc.textFile("Marvel-Names.txt")
super_hero_names=super_hero_names_data.map(names_mapper)
def graph_mapper(line):
    fields=line.split()
    return (int(fields[0]),len(fields)-1)

super_hero_graph_data=sc.textFile("Marvel-Graph.txt")
super_hero_graph=super_hero_graph_data.map(graph_mapper)
super_hero_popularity=super_hero_graph.reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0]))

most_popular_hero=super_hero_popularity.max()

most_popular_hero_name=super_hero_names.lookup(int(most_popular_hero[1]))


print("{a} is the most popular hero. ".format(a=most_popular_hero_name[0]))

