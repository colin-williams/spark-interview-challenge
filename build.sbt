name := "secureworks-spark-demo"

version := "0.0.1"

scalaVersion := "2.12.12"

lazy val app = (project in file("app")).settings(
    assemblyPackageScala / assembleArtifact := false,
    assembly / assemblyJarName := "uber.jar",
    assembly / mainClass := Some("com.seattlesoft.SparkApp"),
    // more settings here ...
  )

// resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-hadoop-cloud" % "3.1.1.3.1.7270.0-253"
//libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.0"
//libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.0"
//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.0"
//libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.901"

libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

