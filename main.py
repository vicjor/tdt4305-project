from task1 import task1
from task2 import task2
from task3 import task3
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
from sys import argv
import argparse
from constants import *


def init_spark():
    spark = SparkSession.builder.master(
        "local[*]").appName("Big Data Project 2021").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    return spark, sc


def main():
    # Use --input_path as input argument to program
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", "-ip", type=str, default=None)
    args = vars(parser.parse_args())
    dataset_path = args["input_path"]

    spark, sc = init_spark()

    dataset = {}
    for data in DATASET_FILES:
        rdd = sc.textFile(dataset_path + "/" + data).map(lambda line: line.split("\t"))
        dataset[data] = rdd

    task1(sc, dataset)
    task2(sc, dataset)
    task3(spark, sc, dataset)
    # try:
    #     graph = spark.read.csv("graph.csv")
    #     print("First five rows of graph: ")
    #     print(graph.take(5))
    # except:
    #     print("\nFile not found.\n")


if __name__ == "__main__":
    main()
