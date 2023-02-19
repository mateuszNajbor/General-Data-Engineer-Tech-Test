name := "general-data-engineer-tech-test"

version := "0.1"

scalaVersion := "2.13.10"

val sparkVer = "3.3.2"
val sparkCore = "org.apache.spark" %% "spark-core" % sparkVer
val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVer

libraryDependencies ++= Seq(sparkCore, sparkSql)