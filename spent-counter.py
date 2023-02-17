from pyspark import SparkConf, SparkContext
import collections
from operator import add

conf = SparkConf().setMaster("local").setAppName("AmountSpent")
sc = SparkContext(conf = conf)


def parseLine(line):
   fields = line.split(",")
   custIds = int(fields[0])
   amount = float(fields[2])
   return (custIds, amount)

lines = sc.textFile("file:///Users/jgonz471/Desktop/SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)

results = (rdd.reduceByKey(lambda x,y: x+y)).sortBy(lambda y:y[1]).collect()
for result in results:
   print(result)
