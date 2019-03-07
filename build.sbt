import Dependencies._

name := "geotrellis-wri"

scalaVersion := Version.scala
scalaVersion in ThisBuild := Version.scala

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
scalacOptions ++= Seq(
  "-deprecation", "-unchecked", "-feature",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials",
  "-language:experimental.macros",
  "-Ypartial-unification" // Required by Cats
)
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }
addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.4" cross CrossVersion.binary)
addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full)
resolvers ++= Seq(
  "GeoSolutions" at "http://maven.geo-solutions.it/",
  "LT-releases" at "https://repo.locationtech.org/content/groups/releases",
  "LT-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
  "OSGeo" at "http://download.osgeo.org/webdav/geotools/",
   Resolver.bintrayRepo("azavea", "geotrellis")
)

libraryDependencies ++= Seq(
  sparkCore % Provided,
  sparkSQL % Provided,
  sparkHive % Provided,
  hadoopAws % Provided,
  geotrellisSpark,
  geotrellisSparkTestKit % Test,
  geotrellisS3,
  geotrellisShapefile,
  geotrellisGeotools,
  geotrellisVectorTile,
  logging,
  scalatest % Test,
  "org.geotools" % "gt-ogr-bridj" % Version.geotools
    exclude("com.nativelibs4java", "bridj"),
  "com.nativelibs4java" % "bridj" % "0.6.1",
  "com.azavea.geotrellis" %% "geotrellis-contrib-vlm" % "0.9.0",
  "com.azavea.geotrellis" %% "geotrellis-contrib-summary" % "0.1.1",
  "com.monovore"  %% "decline" % "0.5.1"
)

// auto imports for local SBT console
initialCommands in console :=
"""
import java.net._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.gdal._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.vector.io.wkt.WKT
import usbuildings._

val conf = new SparkConf().
setIfMissing("spark.master", "local[*]").
setAppName("Building Footprint Elevation").
set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate
implicit val sc: SparkContext = spark.sparkContext
"""

// settings for local testing
console / fork := true
Test / fork := true
Test / parallelExecution := false
Test / testOptions += Tests.Argument("-oD")
Test / javaOptions ++= Seq("-Xms1024m", "-Xmx8144m")

// Settings for sbt-assembly plugin which builds fat jars for use by spark jobs
test in assembly := {}
assemblyMergeStrategy in assembly := {
  case s if s.startsWith("META-INF/services") => MergeStrategy.concat
  case "reference.conf" | "application.conf"  => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// Settings from sbt-lighter plugin that will automate creating and submitting this job to EMR
import sbtlighter._

sparkEmrRelease             := "emr-5.20.0"
sparkAwsRegion              := "us-east-1"
sparkEmrApplications        := Seq("Spark", "Zeppelin", "Ganglia")
sparkS3JarFolder            := "s3://geotrellis-test/wri/jars"
sparkInstanceCount          := 10
sparkMasterType             := "m4.xlarge"
sparkCoreType               := "m4.xlarge"
sparkMasterPrice            := Some(0.5)
sparkCorePrice              := Some(0.5)
sparkClusterName            := s"geotrellis-wri"
sparkEmrServiceRole         := "EMR_DefaultRole"
sparkInstanceRole           := "EMR_EC2_DefaultRole"
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName("geotrellis-emr")
sparkEmrConfigs             := List(
  EmrConfig("spark").withProperties(
    "maximizeResourceAllocation" -> "true"
  ),
  EmrConfig("spark-defaults").withProperties(
    "spark.driver.maxResultSize" -> "3G",
    "spark.dynamicAllocation.enabled" -> "true",
    "spark.shuffle.service.enabled" -> "true",
    "spark.shuffle.compress" -> "true",
    "spark.shuffle.spill.compress" -> "true",
    "spark.rdd.compress" -> "true",
    "spark.driver.extraJavaOptions" -> "-Djava.library.path=/usr/local/lib",
    "spark.executor.extraJavaOptions" -> "-Djava.library.path=/usr/local/lib",
    "spark.executorEnv.LD_LIBRARY_PATH" -> "/usr/local/lib"
  ),
  EmrConfig("spark-env").withProperties(
    "LD_LIBRARY_PATH" -> "/usr/local/lib"
  ),
  EmrConfig("yarn-site").withProperties(
    "yarn.resourcemanager.am.max-attempts" -> "1",
    "yarn.nodemanager.vmem-check-enabled" -> "false",
    "yarn.nodemanager.pmem-check-enabled" -> "false"
  )
)

enablePlugins(TutPlugin)