import graphframes
from pyspark.sql import SQLContext

from main import init_spark

spark, sc = init_spark()

sqlContext = SQLContext(sc)


# Create a Vertex DataFrame with unique ID column "id"
v = sqlContext.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns
e = sqlContext.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
], ["src", "dst", "relationship"])


# Create a GraphFrame
g = graphframes.GraphFrame(v, e)
