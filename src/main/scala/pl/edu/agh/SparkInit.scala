package pl.edu.agh

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

trait SparkInit {
  val spark: SparkSession = SparkSession.builder()
    .appName("HetioNet Spark")
    .master("local[4]")
    .config("spark.neo4j.bolt.username", "neo4j")
    .config("spark.neo4j.bolt.password", "password")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val sqlContext: SQLContext = spark.sqlContext

  private def init(): Unit = {
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }
  init()
  def close(): Unit = {
    spark.close()
  }
}
