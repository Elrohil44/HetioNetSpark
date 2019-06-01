# Requirements

## 1. Neo4j 3.x

https://neo4j.com/

## 2. HetioNet Database

Follow the instructions from here: https://github.com/hetio/hetionet/tree/master/hetnet/neo4j

### Notes:

-----
It may be required to set `dbms.allow_upgrade=true` in your Neo4j configuration file (on Linux `/etc/neo4j/neo4j.conf`).

## 3. Java and Gradle

# How to start

## 1. Provide database credentials (username and password)

Replace the example values in file
`src/main/scala/pl/edu/agh/SparkInit`
```scala
val spark: SparkSession = SparkSession.builder()
    .appName("HetioNet Spark")
    .master("local[4]")
    .config("spark.neo4j.bolt.username", "neo4j")
    .config("spark.neo4j.bolt.password", "password")
    .getOrCreate()
```

## 2. Run with:

For unix like systems:
```bash
./gradlew run
```
For Windows systems:
```bash
exec gradlew.bat
```