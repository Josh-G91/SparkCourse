from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("AddCust").getOrCreate()
cols = ["cust_id", "prod_id", "amount_spent"]
customers = spark.read.option("header", "false").option("inferSchema", "true")\
.csv("file:///Users/jgonz471/SparkCourse/customer-orders.csv")

print("Inferred Schema")
customers.printSchema()

customers.select(customers[0], customers[2]).groupBy("_c0").agg(func.round(func.sum("_c2"),2).alias("total spent")).sort(func.desc("total spent")).show()

spark.stop()
