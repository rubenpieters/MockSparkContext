organization := "be.rubenpieters"
version := "0.0.1"
scalaVersion := "2.10.6"

val sparkVersion = "1.6.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
  , "org.scalatest" %% "scalatest" % "2.2.6"
  , "org.eclipse.jetty" % "jetty-util" % "9.3.11.v20160721"
)

enablePlugins(JmhPlugin)