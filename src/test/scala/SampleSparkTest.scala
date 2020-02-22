import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SampleSparkTest extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = null;

  // exécuté avant chaque test
  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("SampleSparkTest")
      .setMaster("local") // ici on configure un faux cluster spark local pour le test
    sc = new SparkContext(conf)
  }

  test("sample spark test") {
    val fileName = "samples/object-sample"
    val rdd = sc.textFile(fileName, 2).cache()
    assert(rdd.count() == 50, ", wrong number of lines")
  }

  // exécuté après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }
}
