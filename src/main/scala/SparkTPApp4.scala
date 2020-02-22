import org.apache.spark.{SparkConf, SparkContext}
import Math.{pow, sqrt}

object SparkTPApp4 {
  val compte = "ecoquery"

  def guessLargestMove(inputDir: String, sc: SparkContext) = {
    sc.textFile(inputDir)
      .map(l => l.split(','))
      .map(l => (l(0), sqrt(pow(l(4).toDouble - l(3).toDouble, 2) + pow(l(6).toDouble - l(5).toDouble, 2))))
      .reduce((a, b) => if (a._2 >= b._2) a else b)
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val conf = new SparkConf().setAppName("SparkTPApp4-" + compte)
      val sc = new SparkContext(conf)
      val result = guessLargestMove(args(0), sc)
      println("\n\n\nObjet s'étant le plus déplacé: %s (%s)".format(result._1, result._2))
    } else {
      println("Usage: spark-submit --class SparkTPApp4 /home/" + compte + "/SparkTPApp-correction-assembly-1.0.jar " +
        "hdfs:///user/" + compte + "/repertoire-input")
    }
  }
}
