name := "drools_rules"

version := "0.1"

scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.2"


// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % Test


// https://mvnrepository.com/artifact/org.drools/drools-compiler
libraryDependencies += "org.drools" % "drools-compiler" % "7.17.0.Final"

// https://mvnrepository.com/artifact/org.drools/drools-core
libraryDependencies += "org.drools" % "drools-core" % "7.17.0.Final"

libraryDependencies += "com.drool.final" % "dim_prescriber" % "1.1" from "file:\\C:\\Users\\ankitbhardwaj02\\Downloads\\business.rules-1.0.0-20210203.084340-10.jar"


