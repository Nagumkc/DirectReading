name := "stromspark"

version := "1.0"

scalaVersion := "2.11.8"
parallelExecution in Test := false
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0-preview",
  "org.apache.spark" % "spark-mllib_2.11" % "2.0.0-preview",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0-preview",
  "com.twitter.heron" % "heron-api" % "0.14.4",

  "com.twitter.heron" % "heron-storm" % "0.14.4",

  "org.apache.logging.log4j" % "log4j-api" % "2.6.2",

  "org.apache.logging.log4j" % "log4j-core" % "2.6.2",

  "commons-collections" % "commons-collections" % "3.2.1",

  "com.google.guava" % "guava" % "19.0",

  "org.mongodb" % "mongo-java-driver" % "3.3.0",

  "org.apache.httpcomponents" % "httpcore" % "4.4.5",

  "org.apache.httpcomponents" % "httpclient" % "4.5.2",

  "com.googlecode.json-simple" % "json-simple" % "1.1.1",

  "com.github.scopt" % "scopt_2.10" % "3.4.0",

  //apache kafka client

  "org.apache.kafka" % "kafka-clients" % "0.9.0.0",

  "org.apache.kafka" % "kafka_2.11" % "0.9.0.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models",
  "edu.stanford.nlp" % "stanford-parser" % "3.6.0",
  "com.google.protobuf" % "protobuf-java" % "2.6.1"

)
