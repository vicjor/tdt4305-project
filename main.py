from task1 import task1
from task2 import task2
from task3 import task3
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys

# arg = sys.argv.pop()
# print(arg)


def init_spark():
    spark = SparkSession.builder.appName("Big Data Project 2021").getOrCreate()
    sc = spark.sparkContext
    return spark, sc


if __name__ == "__main__":
    spark, sc = init_spark()
    while True:
        task = input("\nWhich task do you want to run? (1-3) ")
        if task == "1":
            task1(sc)
        elif task == "2":
            task2(sc)
        elif task == "3":
            task3(spark, sc)
