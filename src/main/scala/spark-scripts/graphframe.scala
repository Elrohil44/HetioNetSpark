import org.neo4j.spark._

val neo4j = Neo4j(sc)

val w = 0.4

val graphFrame = neo4j.
  rels("MATCH (a)-[r]-(b) RETURN ID(a) as src, ID(b) as dst, type(r) as relationship").
  nodes("MATCH (n) RETURN ID(n) as id, labels(n)[0] as type", Map.empty).
  loadGraphFrame

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


