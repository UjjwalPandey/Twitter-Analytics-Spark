name := "Twitter-Analytics(Spark)"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided"
)

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"

// https://mvnrepository.com/artifact/org.twitter4j/twitter4j
libraryDependencies += "org.twitter4j" % "twitter4j" % "4.0.7" pomOnly()

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.4.0"


