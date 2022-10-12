name := "reinvent-spark-streaming-demo"
version := "1.0"
scalaVersion := "2.12.15"

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1" % Test
libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.11.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10-assembly" % "3.2.1"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", _*) => MergeStrategy.discard
 case _                        => MergeStrategy.first
}
