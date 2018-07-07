name := "ScalaDemo"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.clapper" %% "grizzled-slf4j" % "1.3.2",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.2.0",
  "com.databricks" %% "spark-xml" % "0.4.1",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "org.apache.flink" %% "flink-scala" % "1.4.2",
"org.apache.flink" %% "flink-streaming-scala" % "1.4.2",
"org.apache.flink" %% "flink-clients" % "1.4.2"
)
