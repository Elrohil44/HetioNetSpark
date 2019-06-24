import org.apache.spark.rdd.RDD
import scala.util.hashing.MurmurHash3
import spark.implicits._
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.{DataFrame, Row}
import org.graphframes.GraphFrame

val w = 0.4

val nodes = spark.read.format("csv").option("header", "true").load("resources/nodes.csv")
val edges = spark.read.format("csv").option("header", "true").load("resources/edges.csv")

val nodesRDD: RDD[(VertexId, (String, String))] = {
  nodes.map(node => (MurmurHash3.stringHash(node(0).toString).toLong, (node(1).toString, node(2).toString))).rdd
}
val edgesRDD: RDD[Edge[String]] = {
  edges.flatMap(edge => Seq(
    Edge(MurmurHash3.stringHash(edge(0).toString).toLong, MurmurHash3.stringHash(edge(2).toString).toLong, edge(1).toString),
    Edge(MurmurHash3.stringHash(edge(2).toString).toLong, MurmurHash3.stringHash(edge(0).toString).toLong, edge(1).toString)
  )).rdd
}

val defaultValue = ("name", "kind")
val graph = Graph(nodesRDD, edgesRDD, defaultValue)
val graphFrame = GraphFrame.fromGraphX(graph)

val degreesMap = graphFrame.edges.
  map(row => ((row.getLong(0), row.getString(2)), 1)).
  rdd.
  reduceByKey(_ + _).
  map(t => (t._1._2, (t._1._1, t._2))).
  groupByKey().map(t => (t._1, t._2.toSeq.toMap)).
  collect().
  toSeq.
  toMap


val exampleMetapaths = List(
  List("Compound", "CbG", "Gene", "CbG", "Compound", "CtD", "Disease"),
  List("Compound", "CbG", "Gene", "DaG", "Disease"),
  List("Compound", "PCiC", "PharmacologicClass", "PCiC", "Compound", "CtD", "Disease"),
  List("Compound", "CrC", "Compound", "CtD", "Disease"),
  List("Compound", "CtD", "Disease", "DrD", "Disease"),
  List("Compound", "CrC", "Compound", "CrC", "Compound", "CtD", "Disease"),
  List("Compound", "CpD", "Disease", "CpD", "Compound", "CtD", "Disease"),
  List("Compound", "CbG", "Gene", "GpPW", "Pathway", "GpPW", "Gene", "DaG", "Disease"),
  List("Compound", "CbG", "Gene", "AeG", "Anatomy", "DlA", "Disease"),
  List("Compound", "CrC", "Compound", "CbG", "Gene", "DaG", "Disease"),
  List("Compound", "CbG", "Gene", "CdG", "Compound", "CrC", "Compound", "CtD", "Disease")
)

def getMotifs(graphFrame: GraphFrame, metapath: List[String]): DataFrame = {
  def getMotifString(metapath: List[String], ndx: Int = 0, motifString: String = ""): String = metapath match {
    case n0 :: r0 :: n1 :: Nil => s"$motifString(n$ndx)-[r$ndx]->(n${ndx + 1})"
    case n0 :: r0 :: n1 :: rest => getMotifString(n1 :: rest, ndx + 1, s"$motifString(n$ndx)-[r$ndx]->(n${ndx + 1});")
  }
  def getDataFrame(
                    metapath: List[String],
                    ndx: Int = 0,
                    dataFrame: DataFrame = graphFrame.find(getMotifString(metapath))): DataFrame = metapath match {
    case n0 :: r0 :: rest => getDataFrame(rest, ndx + 1, dataFrame.filter(s"n$ndx.attr._2 = '$n0'").filter(s"r$ndx.attr = '$r0'"))
    case n0 :: Nil => dataFrame.
      filter(s"n$ndx.attr._2 = '$n0'").
      filter("n0.attr._1 = 'Furosemide'")
  }

  getDataFrame(metapath)
}

def calculatePDP(path: List[Row], ndx: Long = 0, pdp: Double = 1): Double = path match {
  case x :: y :: rest => ndx % 2 match {
    case 0 => calculatePDP(y :: rest, ndx + 1, pdp * scala.math.pow(degreesMap(y.getString(2))(x.getLong(0)), -w))
    case _ => calculatePDP(y :: rest, ndx + 1, pdp * scala.math.pow(degreesMap(x.getString(2))(y.getLong(0)), -w))
  }
  case node :: Nil => pdp
}

val dwpcsRdd = exampleMetapaths.map(metapath => getMotifs(graphFrame, metapath).
  map(row => (
    (row.getStruct(0).getStruct(1).getString(0), row.getStruct(row.length - 1).getStruct(1).getString(0)),
    calculatePDP(row.toSeq.toList.asInstanceOf[List[Row]])
  )).
  rdd.
  reduceByKey(_ + _)
)

val DWPCs = dwpcsRdd.map(_.collectAsync).map(_.get)