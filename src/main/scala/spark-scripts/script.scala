import org.neo4j.spark._

val neo4j = Neo4j(sc)

val relationshipsQuery = neo4j.cypher("MATCH (n)-[r]-() RETURN ID(n) as id, type(r) as relationship, COUNT(r) as degree")
val relationshipsWithDegreesRDD =  relationshipsQuery.loadRowRdd

val relationshipsWithDegrees = relationshipsWithDegreesRDD.map(r => (r.getAs[String](1), (r.getAs[Long](0), r.getAs[Long](2)))).groupByKey().map(t => (t._1, Map(t._2.toList: _*))).collect()
val relationshipsMap = Map(relationshipsWithDegrees.toList: _*)



