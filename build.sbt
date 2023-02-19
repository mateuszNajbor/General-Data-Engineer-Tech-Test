name := "general-data-engineer-tech-test"

version := "0.1"

scalaVersion := "2.13.10"

val sparkVer = "3.3.2"
val sparkCore = "org.apache.spark" %% "spark-core" % sparkVer
val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVer
val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15" % Test

libraryDependencies ++= Seq(
  sparkCore,
  sparkSql,
  scalaTest)