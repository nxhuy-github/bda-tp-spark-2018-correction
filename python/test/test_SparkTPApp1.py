# -*- coding: utf-8 -*-

import pytest

import SparkTPApp1

@pytest.mark.spark
def test_compteAB(spark_session):
    compte = "ecoquery"
    logFile = "README.md"
    (nbA, nbB) = SparkTPApp1.compteAB(logFile, spark_session)
    assert 154 == nbA, "wrong number of a"
    assert 63 == nbB, "wrong number of b"

