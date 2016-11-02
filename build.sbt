organization := "be.rubenpieters"
version := "0.0.1"
scalaVersion := "2.11.8"

val sparkVersion = "1.6.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
  , "org.scalatest" %% "scalatest" % "2.2.6"
)

enablePlugins(JmhPlugin)