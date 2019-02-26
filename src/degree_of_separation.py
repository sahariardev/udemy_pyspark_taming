from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("local").setAppName("Degree of Seperation")
sc=SparkContext().getOrCreate(conf=conf)

startCharacterId=5306
targerId=14
counter=sc.accumulator(0)
local_counter=0

color_dict={
    'WHITE':0,
    'GRAY':1,
    'BLACK':2
}
color_reverse_dict={
    0:'WHITE',
    1:'GRAY',
    2:'BLACK'
}

def convert_to_int_list(items):
    new_list=[]
    for item in items:
        if len(item) !=0:
            new_list.append(int(item))
    return  new_list

def convertToBFS(line):
    fields=line.split(" ")
    heroId=int(fields[0])
    connections=convert_to_int_list(fields[1:])
    color='WHITE'
    distance=9999
    if(heroId== startCharacterId):
        color='GRAY'
        distance=0
    return (heroId,(connections,distance,color))
fileRDD=sc.textFile("Marvel-Graph.txt")
mapped=fileRDD.map(convertToBFS)

def BFS_map(node):
    u=node[0]
    connextions=node[1][0]
    distance=node[1][1]
    color=node[1][2]
    results=[]
    if(color == 'GRAY'):
        for v in connextions:
            new_distance=distance+1
            new_color='GRAY'
            newEntry=((v,([],new_distance,new_color)))
            results.append(newEntry)
            if(v==targerId):
                counter.add(1)
                local_counter=1
        color='BLACK'
    results.append((u,(connextions,distance,color)))
    return results



def BFS_reducer(data1,data2):
    edges=[]
    if len(data1[0])!=0:
        edges.extend(data1[0])
    if len(data2[0])!=0:
        edges.extend(data2[0])
    distance=min(data1[1],data2[1])
    color=color_reverse_dict[max(color_dict[data1[2]],color_dict[data2[2]])]
    return (edges,distance,color)




while True:
     mapped = mapped.flatMap(BFS_map)
     mapped.count()
     if counter.value != 0 or local_counter != 0:
         break
     mapped=mapped.reduceByKey(BFS_reducer)

mapped=mapped.reduceByKey(BFS_reducer)

def printT(x):
    if(x[0]==targerId):
        print("")
        print("The degree of seperation is {x}".format(x=x[1][1]))
#printing the result
mapped.foreach(lambda x:printT(x))
