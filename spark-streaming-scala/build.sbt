name := "spark-streaming-scala"

version := "0.1"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "org.apache.spark" %% "spark-streaming" % "2.4.7",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.7",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "2.4.7",
  "net.liftweb" %% "lift-json" % "3.4.2",
  "net.debasishg" %% "redisclient" % "3.30",
  "com.redislabs" % "spark-redis_2.12" % "2.4.2",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.1",
  "com.novocode" % "junit-interface" % "0.11" % Test
)
