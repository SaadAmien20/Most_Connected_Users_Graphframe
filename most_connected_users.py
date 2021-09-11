from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import col
from graphframes import *

spark = SparkSession.builder.appName('JumiaTask').getOrCreate()

schema = StructType([
    StructField("src", IntegerType(), True), 
    StructField("dst", IntegerType(), True)
])

edges = spark.read.csv("/media/saad/943851B3385194D81/jumia/higgs-social_network.edgelist",sep=' ',header=False,schema=schema)
vertices = edges.select('src').distinct().withColumnRenamed('src',"id")

g = GraphFrame(vertices,edges)

result = g.inDegrees
result_order =result.sort(result.inDegree.desc())

most_connected_user = result_order.withColumnRenamed('id','users').withColumnRenamed('inDegree','connected_users')
most_connected_user.show(370341)

# fetch_all = most_connected_user.collect()
# for i in fetch_all:
#     print (i)