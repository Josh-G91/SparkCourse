from __future__ import print_function
import pyspark.sql.functions as F
from pyspark.ml.regression import DecisionTreeRegressor

from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

    # Load up our data and convert it to the format MLLib expects.
    inputLines = spark.read.option("header", True).csv("/Users/jgonz471/SparkCourse/realestate.csv")
    inputLines.printSchema()
    new_df = inputLines.withColumn("HouseAge", F.col("HouseAge").cast("float")).withColumn("DistanceToMRT", F.col("DistanceToMRT").cast("float")).withColumn("NumberConvenienceStores", F.col("NumberConvenienceStores").cast("float"))\
    .withColumn("PriceOfUnitArea", F.col("PriceOfUnitArea").cast("float"))
    new_df.printSchema()
    assembler = VectorAssembler(inputCols=["HouseAge","DistanceToMRT", "NumberConvenienceStores"],outputCol="features")
    df = assembler.transform(new_df)
    df.show()
    final = df.select(["HouseAge","DistanceToMRT", "NumberConvenienceStores", "features", "PriceOfUnitArea"])
    train ,test = final.randomSplit([0.5,0.5])
    	
    df_classifier = DecisionTreeRegressor(labelCol="PriceOfUnitArea").fit(train)
    df_pred = df_classifier.transform(test)
    df_pred.show()
    
    spark.stop()
