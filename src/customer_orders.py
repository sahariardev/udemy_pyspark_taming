'''
  This program will count total amount of purchases is made by individuals
'''

from pyspark import  SparkContext,SparkConf

def exchange_key_value_order(x):
    return (x[1],x[0])

def extractCustomerIdAndPurchaseAmount(line):
    data=line.split(",")
    return (int(data[0]),float(data[2]))

conf=SparkConf().setMaster("local").setAppName("Customers Order")
sc=SparkContext().getOrCreate(conf=conf)

data=sc.textFile("customer-orders.csv")

customer_purchase=data.map(extractCustomerIdAndPurchaseAmount).reduceByKey(lambda x,y:x+y)
results=customer_purchase.collect()

for result in results:
    print("User id : {a} Amount Spent: {b:.2f} USD ".format(a=result[0],b=result[1]))


