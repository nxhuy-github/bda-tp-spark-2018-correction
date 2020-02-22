#! python
# -*- coding: utf-8 -*-
import sys
import pyspark
from math import sqrt, pow

compte = "ecoquery"


def guessLargestMove(inputDir, sc):
    raw_data = sc.textFile(inputDir)
    splitted = raw_data.map(lambda l: l.split(","))
    distances = splitted.map(
        lambda l: (
            l[0],
            sqrt(pow(float(l[4]) - float(l[3]), 2) + pow(float(l[6]) - float(l[5]), 2)),
        )
    )

    def maxt(a, b):
        if a[1] >= b[1]:
            return a
        else:
            return b

    return distances.reduce(maxt)


if __name__ == "__main__":
    args = sys.argv
    if len(args) > 1 and "-h" not in args and "--help" not in args:
        sc = pyspark.SparkContext(appName="SparkTPApp4-" + compte)
        result = guessLargestMove(args[1], sc)
        print(
            "\n\n\nObjet s'étant le plus déplacé: {0} ({1})\n\n".format(
                result[0], result[1]
            )
        )
    else:
        print(
            "Usage: spark-submit "
            + args[0]
            + " hdfs:///user/"
            + compte
            + "/repertoire-donnees"
        )
