#! python
# -*- coding: utf-8 -*-
import sys
import pyspark
import petasky_schema

compte = "ecoquery"


def lineToStrings(l):
    return list(map(lambda c: c.strip(), l.split(",")))


def buildTuple(r):
    oId = r[petasky_schema.s_objectId()]
    sId = r[petasky_schema.s_sourceId()]
    ra = float(r[petasky_schema.s_ra()])
    decl = float(r[petasky_schema.s_decl()])
    return (oId, 1, sId, ra, ra, decl, decl)


def aggTuples(t1, t2):
    return (
        t1[0],  # oId
        t1[1] + t2[1],  # count
        max(t1[2], t2[2]),  # max sourceId
        min(t1[3], t2[3]),  # min ra
        max(t1[4], t2[4]),  # max ra
        min(t1[5], t2[5]),  # min decl
        max(t1[6], t2[6]),  # max decl
    )


def aggregateByObjectId(inputDir, sc):
    rows = sc.textFile(inputDir).map(lineToStrings)
    noNulls = rows.filter(lambda r: r[petasky_schema.s_objectId()] != "NULL")
    filteredCols = noNulls.map(buildTuple)
    keyed = filteredCols.map(lambda t: (t[0], t))
    agg = keyed.reduceByKey(aggTuples)
    return agg.map(lambda p: p[1])  # extract the resulting tuples


if __name__ == "__main__":
    args = sys.argv
    if len(args) > 2 and "-h" not in args and "--help" not in args:
        sc = pyspark.SparkContext(appName="SparkTPApp3-" + compte)
        result = aggregateByObjectId(args[1], sc)
        result.map(lambda t: ",".join(map(str, t))).saveAsTextFile(args[2])
    else:
        print(
            "Usage: spark-submit --py-files petasky_schema.py "
            + "--files Source.sql,Object.sql "
            + args[0]
            + " hdfs:///user/"
            + compte
            + "/repertoire-donnees"
            + " hdfs:///user/"
            + compte
            + "/repertoire-resultat"
        )
