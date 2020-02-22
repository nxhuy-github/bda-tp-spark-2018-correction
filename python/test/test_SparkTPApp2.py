# -*- coding: utf-8 -*-

import pytest

import SparkTPApp2


@pytest.mark.spark
def test_compte_sample_source(spark_context):
    spark_context.addPyFile("python/petasky_schema.py")
    spark_context.addFile("python/Source.sql")
    spark_context.addFile("python/Object.sql")
    countRDD = SparkTPApp2.countByObjectId("samples/source-sample", spark_context)
    countById = countRDD.collectAsMap()
    assert countById["430209694171136"] == 6
    assert len(countById) == 1
    assert "NULL" not in countById

