# -*- coding: utf-8 -*-

import pytest

@pytest.mark.spark
def test_sample(spark_session):
    """Test simple utilisant le fichier object-sample.
    
       L'argument spark_session permet d'obtenir la session spark.
       Cet argument sera fourni par le plugin pytest-spark 
       (cf https://pypi.org/project/pytest-spark/)

       L'annotation @pytest.mark.spark va empêcher l'exécution de ce test
       sauf dans le cas ou on lance les tests avec l'option --on-spark
       Cela permettra d'exécuter les autres test en dehors du cluster.
    """
    fileName = "samples/object-sample"
    df = spark_session.read.text(fileName).cache()
    assert 50 == df.count(), ", wrong number of lines"


def test_sans_spark():
    """Exemple de test qui n'utilise pas Spark

       On peut remarquer qu'il n'y a pas d'argument spark_session
    """
    filename = "samples/object-sample"
    with open(filename) as f:
        data = list(f)
        assert 50 == len(data), ", wrong number of lines"
