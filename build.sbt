organization := "be.rubenpieters"
version := "0.0.1"
scalaVersion := "2.10.6"

val sparkVersion = "1.6.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
  , "org.scalatest" %% "scalatest" % "2.2.6"
  , "org.eclipse.jetty" % "jetty-server" % "8.1.14.v20131031"
  , "org.eclipse.jetty" % "jetty-servlet" % "8.1.14.v20131031"
  , "org.mockito" % "mockito-all" % "1.10.19"
  , "org.scalacheck" %% "scalacheck" % "1.12.6" % "test"

)

enablePlugins(JmhPlugin)