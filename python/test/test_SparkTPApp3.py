# -*- coding: utf-8 -*-

import pytest

import SparkTPApp3


@pytest.mark.spark
def test_compte_sample_source(spark_context):
    spark_context.addPyFile("python/petasky_schema.py")
    spark_context.addFile("python/Source.sql")
    spark_context.addFile("python/Object.sql")
    spark_context.addPyFile("python/SparkTPApp3.py")
    result = SparkTPApp3.aggregateByObjectId(
        "samples/source-sample", spark_context
    ).cache()
    assert result.count() == 1
    agg = result.collect()
    agg = agg[0]
    assert agg[0] == "430209694171136"
    assert agg[1] == 6
    assert agg[2] == "29809086638981597"
    assert abs(agg[3] - 357.9893337673741) < 0.00000000000001
    assert abs(agg[4] - 357.9894411124743) < 0.00000000000001
    assert abs(agg[5] - 2.5646291352701804) < 0.00000000000001
    assert abs(agg[6] - 2.564762701468148) < 0.00000000000001

