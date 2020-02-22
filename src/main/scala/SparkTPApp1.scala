import org.apache.spark.{SparkConf}
import org.apache.spark.sql.SparkSession

object SparkTPApp1 {

  val compte = "ecoquery"

  def compteAB(logFile: String, sc: SparkSession): (Long, Long) = {
    val logData = sc.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    (numAs, numBs)
  }

  def main(args: Array[String]): Unit = {
    val logFile = "hdfs:///user/" + compte + "/README.md"
    val sc = SparkSession.builder.appName("SparkTPApp1-" + compte).getOrCreate()
    val (nbA, nbB) = compteAB(logFile, sc)
    println("\n\nLines with a: %s, Lines with b: %s\n\n".format(nbA, nbB))
  }

}
