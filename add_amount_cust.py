
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def remap_data(input):
    inp = input.split(",")
    id = int(inp[0])
    cost = float(inp[2])
    return (id,cost) 

input = sc.textFile("file:///Users/jgonz471/SparkCourse/customer-orders.csv")
updated_data = input.map(remap_data)
red = updated_data.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ) 
for data in red.collect():
    print(data)
