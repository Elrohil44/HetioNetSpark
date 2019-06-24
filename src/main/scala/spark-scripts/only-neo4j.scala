import org.neo4j.spark._

val neo4j = Neo4j(sc)

val w = 0.4

def getQuery(path: List[String]): String = {
  def getMatch(list: List[String], ndx: Int = 0, acc: String = ""): String = list match {
    case s :: s2 :: rest => getMatch(rest, ndx + 1, s"$acc(n$ndx:$s)-[r$ndx:$s2]-")
    case s :: Nil => getMatch(Nil, ndx + 1, s"$acc(n$ndx:$s)")
    case Nil => acc
  }

  def getWith(list: List[String], ndx: Int = 0, acc: String = ""):String = list match {
    case s :: s2 :: s3 :: Nil => getWith(Nil, ndx + 1,
      s"""${acc}size((n$ndx)-[:$s2]-()),
         |size(()-[:$s2]-(n${ndx + 1}))
         |
       """.stripMargin)
    case s :: s2 :: rest => getWith(rest, ndx + 1,
      s"""${acc}size((n$ndx)-[:$s2]-()),
         |size(()-[:$s2]-(n${ndx + 1})),
         |
       """.stripMargin)
    case Nil => acc
  }

  s"""
     |MATCH path = ${getMatch(path)}
     |WHERE n0.name = "Furosemide"
     |WITH [
     |${getWith(path)}
     |] AS degrees, path, n0 as compound, n${path.length / 2} as disease
     |WITH
     |sum(reduce(pdp = 1.0, d in degrees| pdp * d ^ -${w})) AS DWPC,
     |compound,
     |disease
     |RETURN ID(compound), ID(disease), DWPC
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

val DWPCs = pathsRDDs.map(rdd => rdd.collectAsync).map(_.get)
