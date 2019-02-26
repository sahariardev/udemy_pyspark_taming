from pyspark import SparkContext,SparkConf

def line_parser(line):
    data=line.split(",")
    station=data[0]
    flag=data[2]
    temp=float(data[3])*.1*(9/5)+32
    return (station,flag,temp)


conf=SparkConf().setMaster("local").setAppName("Temperature Calculation")
sc=SparkContext().getOrCreate(conf=conf)
data=sc.textFile("1800.csv")
paresed_data=data.map(line_parser)

min_temp=paresed_data.filter(lambda x:x[1]=='TMAX')
stationTemp=min_temp.map(lambda  x:(x[0],x[2]))
minTemp=stationTemp.reduceByKey(lambda x,y:max(x,y))
results=minTemp.collect();
for r in results:
    print("station id is {a} and min temp is {b:.2f} F".format(a=r[0],b=r[1]))