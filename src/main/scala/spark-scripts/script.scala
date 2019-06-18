import org.neo4j.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

val neo4j = Neo4j(sc)

val relationshipsQuery = neo4j.cypher("MATCH (n)-[r]-() RETURN ID(n) as id, type(r) as relationship, COUNT(r) as degree")
val relationshipsWithDegreesRDD =  relationshipsQuery.loadRowRdd

val relationshipsWithDegrees = relationshipsWithDegreesRDD.
  map(r => (r.getAs[String](1), (r.getAs[Long](0), r.getAs[Long](2)))).
  groupByKey().
  map(t => (t._1, t._2.toSeq.toMap)).
  collect()

val relationshipsMap = relationshipsWithDegrees.toSeq.toMap

val w = 0.4

def getQuery(path: List[String]): String = {
  def getMatch(list: List[String], ndx: Int = 0, acc: String = ""): String = list match {
    case s :: s2 :: rest => getMatch(rest, ndx + 1, s"$acc(n$ndx:$s)-[r$ndx:$s2]-")
    case s :: Nil => getMatch(Nil, ndx + 1, s"$acc(n$ndx:$s)")
    case Nil => acc
  }

  def getReturn(list: List[String], ndx: Int = 0, acc: String = ""):String = list match {
    case s :: s2 :: rest => getReturn(rest, ndx + 1, s"${acc}ID(n$ndx), type(r$ndx), ")
    case s :: Nil => getReturn(Nil, ndx + 1, s"${acc}ID(n$ndx)")
    case Nil => acc
  }

  s"""
     |MATCH ${getMatch(path)}
     |WHERE n0.name = "Furosemide"
     |RETURN ${getReturn(path)}
   """.stripMargin
}

val exampleMetapaths = List(
  List("Compound", "BINDS_CbG", "Gene", "BINDS_CbG", "Compound", "TREATS_CtD", "Disease"),
  List("Compound", "BINDS_CbG", "Gene", "ASSOCIATES_DaG", "Disease"),
  List("Compound", "INCLUDES_PCiC", "PharmacologicClass", "INCLUDES_PCiC", "Compound", "TREATS_CtD", "Disease"),
  List("Compound", "RESEMBLES_CrC", "Compound", "TREATS_CtD", "Disease"),
  List("Compound", "TREATS_CtD", "Disease", "RESEMBLES_DrD", "Disease"),
  List("Compound", "RESEMBLES_CrC", "Compound", "RESEMBLES_CrC", "Compound", "TREATS_CtD", "Disease"),
  List("Compound", "PALLIATES_CpD", "Disease", "PALLIATES_CpD", "Compound", "TREATS_CtD", "Disease"),
  List("Compound", "BINDS_CbG", "Gene", "PARTICIPATES_GpPW", "Pathway", "PARTICIPATES_GpPW", "Gene", "ASSOCIATES_DaG", "Disease"),
  List("Compound", "BINDS_CbG", "Gene", "EXPRESSES_AeG", "Anatomy", "LOCALIZES_DlA", "Disease"),
  List("Compound", "RESEMBLES_CrC", "Compound", "BINDS_CbG", "Gene", "ASSOCIATES_DaG", "Disease"),
  List("Compound", "BINDS_CbG", "Gene", "DOWNREGULATES_CdG", "Compound", "RESEMBLES_CrC", "Compound", "TREATS_CtD", "Disease")
)


val pathsRDDs = exampleMetapaths.map(metapath => neo4j.
  cypher(getQuery(metapath)).
  loadRowRdd
)

def calculateDWPCs(pathRdds: List[RDD[Row]]): List[RDD[((Long, Long), Double)]] = {
  val weight = w
  def calculatePDP(path: List[Any], degrees: Map[String, Map[Long, Long]], ndx: Long = 0, pdp: Double = 1): Double = path match {
    case x :: y :: rest => ndx % 2 match {
      case 0 => calculatePDP(y :: rest, degrees, ndx + 1, pdp * scala.math.pow(degrees(y.asInstanceOf[String])(x.asInstanceOf[Long]), -weight))
      case _ => calculatePDP(y :: rest, degrees, ndx + 1, pdp * scala.math.pow(degrees(x.asInstanceOf[String])(y.asInstanceOf[Long]), -weight))
    }
    case node :: Nil => pdp
  }

  val degrees = relationshipsMap
  pathRdds.map(rdd => rdd.
    map(path => {
      val pathAsList = path.toSeq.toList
      ((path.getLong(0), path.getLong(path.length - 1)), calculatePDP(pathAsList, degrees))
    }).
    reduceByKey(_ + _))
}

val DWPCs = calculateDWPCs(pathsRDDs).map(rdd => rdd.collectAsync).map(_.get)
