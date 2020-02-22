#! python
# -*- coding: utf-8 -*-
import sys
import pyspark
import petasky_schema

compte = "ecoquery"


def countByObjectId(inputFilename, sc):
    return (
        sc.textFile(inputFilename)
        .map(  # transformer chaque ligne en un tableau de String
            lambda l: list(map(lambda c: c.strip(), l.split(",")))
        )
        .filter(lambda l: l[petasky_schema.s_objectId()] != "NULL")
        .map(lambda l: (l[petasky_schema.s_objectId()], 1))  # on associe 1 à chaque clé
        .reduceByKey(lambda count1, count2: count1 + count2)
    )


def print_row(r):
    print(r)


if __name__ == "__main__":
    args = sys.argv
    if len(args) > 1 and args[1] not in ["-h", "--help"]:
        sc = pyspark.SparkContext(appName="SparkTPApp2-"+compte)
        result = countByObjectId(args[1], sc)
        if len(args) > 2:
            result.map(lambda r: str(r[0])+","+str(r[1])).saveAsTextFile(args[2])
        else:
            result.collect().foreach(print_row)
    else:
        print(
            "Usage: spark-submit --py-files petasky_schema.py --files Source.sql,Object.sql "
            + args[0]
            +" hdfs:///user/"
            + compte
            + "/fichier-a-lire.csv "
            + "[hdfs:///user/"
            + compte
            + "/repertoire-resultat]"
        )

