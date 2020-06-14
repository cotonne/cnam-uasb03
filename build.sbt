import Dependencies._
import sbt.Keys.libraryDependencies

val sparkVersion = "2.4.5"

ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "fr.cnam.uasb03"
ThisBuild / organizationName := "intrusion-detector"

// assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
// assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion}_${version.value}.jar"


lazy val root = (project in file("."))
  .settings(
    name := "Intrusion-Detector",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "com.influxdb" % "influxdb-client-scala" % "1.8.0",
    libraryDependencies += "com.maxmind.geoip2" % "geoip2" % "2.13.1",
    libraryDependencies += "nl.basjes.parse.useragent" % "yauaa" % "5.17",
    libraryDependencies ++= {
      Seq(
        "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
        //          "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
      )
    }
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
