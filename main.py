import task1
import task2
import task3
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    sparkConf = SparkConf().setAppName("Big Data Project 2021").setMaster("local")
    sc = SparkContext(conf=sparkConf)
    while True:
        task = input("Which task do you want to run? (1-3) ")
        if task == "1":
            task1.task1(sc)
        elif task == "2":
            task2.task2(sc)
        elif task == "3":
            task3.task3(sc)
