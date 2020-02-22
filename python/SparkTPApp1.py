# -*- coding: utf-8 -*-
import sys


compte = "ecoquery"

def compteAB(logFile, sc):
    logData = sc.read.text(logFile).cache()
    numAs = logData.filter(logData.value.contains("a")).count()
    numBs = logData.filter(logData.value.contains("b")).count()
    return (numAs, numBs)

def getSpark(appName):
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName(appName).getOrCreate()

if __name__ == "__main__":
    logFile = "hdfs:///user/" + compte + "/README.md"
    appName = "SparkTPApp1-" + compte
    spark = getSpark(appName)
    (nbA, nbB) = compteAB(logFile, spark)
    print("\n\nLines with a: {}, Lines with b: {}\n\n".format(nbA, nbB))
    spark.stop()
