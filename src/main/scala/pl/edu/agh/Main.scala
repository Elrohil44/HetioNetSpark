package pl.edu.agh

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.util.hashing.MurmurHash3

object Main extends SparkInit {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    log("nodes loading ...")
    val nodes = spark.read.format("csv").option("header", "true").load("src/main/scala/pl/edu/agh/nodes.csv")
    log("nodes loaded")
    log("edges loading ...")
    val edges = spark.read.format("csv").option("header", "true").load("src/main/scala/pl/edu/agh/edges.csv")
    log("edges loaded")

    val nodesRDD: RDD[(VertexId, (String, String))] = {
      nodes.map(node => (MurmurHash3.stringHash(node(0).toString).toLong, (node(1).toString, node(2).toString))).rdd
    }
    log("nodes rdd")
    val edgesRDD: RDD[Edge[String]] = {
      edges.map(edge => Edge(MurmurHash3.stringHash(edge(0).toString).toLong, MurmurHash3.stringHash(edge(2).toString).toLong, edge(1).toString)).rdd
    }
    log("edges rdd")

    val defaultValue = ("name", "kind")
    val graph = Graph(nodesRDD, edgesRDD, defaultValue)
    log("graph created")

    log(graph.numVertices)
    log(graph.numEdges)
  }
  
  def log(text: Any): Unit = {
    println(new SimpleDateFormat("[YYYY_MM_dd_HH:mm:ss] ").format(new Date) + text)
  }
}
