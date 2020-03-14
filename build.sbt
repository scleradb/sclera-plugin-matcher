name := "sclera-plugin-matcher"

description := "Add-on package that enables Sclera to efficiently and flexibly analyze ordered streaming data"

version := "4.0-SNAPSHOT"

homepage := Some(url("https://github.com/scleradb/sclera-plugin-matcher"))

organization := "com.scleradb"

organizationName := "Sclera, Inc."

organizationHomepage := Some(url("https://www.scleradb.com"))

startYear := Some(2012)

scalaVersion := "2.13.1"

licenses := Seq("Apache License version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

libraryDependencies ++= Seq(
    "com.scleradb" %% "sclera-core" % "4.0-SNAPSHOT" % "provided",
    "com.scleradb" %% "sclera-automata" % "4.0-SNAPSHOT" % "provided",
    "com.scleradb" %% "sclera-config" % "4.0-SNAPSHOT" % "test",
    "com.scleradb" %% "sclera-jdbc" % "latest.integration" % "test",
    "com.scleradb" %% "sclera-sqltests-runner" % "4.0-SNAPSHOT" % "test",
    "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)

scalacOptions ++= Seq(
    "-Werror", "-feature", "-deprecation", "-unchecked"
)

exportJars := true

fork in Test := true
