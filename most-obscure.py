from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///Users/jgonz471/SparkCourse/Marvel+Names")

lines = spark.read.text("file:///Users/jgonz471/SparkCourse/Marvel+Graph")
lines.show()
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0])\
.withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1)\
.groupBy("id").agg(func.sum("connections").alias("connections"))

oneConnect = (connections.filter(func.col("connections") == 1))
names.join(oneConnect,names["id"] == oneConnect["id"]).select("name").show()

#print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

