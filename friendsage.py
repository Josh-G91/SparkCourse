from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///Users/jgonz471/Desktop/SparkCourse/fakefriends-header.csv")

print("Here is our inferred schema:")
people.printSchema()

people.select(people.age, people.friends).groupBy("age").avg("friends").show()

spark.stop()
