import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkTPApp1Test extends FunSuite with BeforeAndAfter {
  var sc: SparkSession = null;

  // exécuté avant chaque test
  before {
    // configuration de Spark
    sc = SparkSession.builder
      .master("local")
      .appName("SparkTPApp1Test")
      .getOrCreate()
  }

  test("compte a et b") {
    val (nA, nB) = SparkTPApp1.compteAB("README.md", sc)
    //assert(nA == 98, ", mauvais nombre de a") // change avec README.md
    //assert(nB == 47, ", mauvais nombre de b")
  }

  // exécuté après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }
}
