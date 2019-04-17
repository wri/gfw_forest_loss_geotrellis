import Dependencies._

name := "treecoverloss"
organization := "org.globalforestwatch"
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

scalaVersion := Version.scala
scalaVersion in ThisBuild := Version.scala

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
  decline,
  scalatest % Test,
  "org.geotools" % "gt-ogr-bridj" % Version.geotools
    exclude("com.nativelibs4java", "bridj"),
  "com.nativelibs4java" % "bridj" % "0.6.1",
  "com.azavea.geotrellis" %% "geotrellis-contrib-vlm" % "0.9.0",
  "com.azavea.geotrellis" %% "geotrellis-contrib-summary" % "0.1.1",
  "org.scalanlp" %% "breeze" % "0.13.2",
  "org.scalanlp" %% "breeze-natives" % "0.13.2",
  "org.scalanlp" %% "breeze-viz" % "0.13.2"
)

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

// auto imports for local SBT console
// can be used with `test:console` command
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
import geotrellis.contrib.vlm.geotiff._
import geotrellis.vector.io.wkt.WKT

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import org.globalforestwatch.treecoverloss._
import org.globalforestwatch.util._


val conf = new SparkConf().
setIfMissing("spark.master", "local[*]").
setAppName("Tree Cover Loss Console").
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
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case PathList("META-INF", xs@_*) =>
    xs match {
      case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
      // Concatenate everything in the services directory to keep GeoTools happy.
      case ("services" :: _ :: Nil) =>
        MergeStrategy.concat
      // Concatenate these to keep JAI happy.
      case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
        MergeStrategy.concat
      case (name :: Nil) => {
        // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
        if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF"))
          MergeStrategy.discard
        else
          MergeStrategy.first
      }
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}

// Settings from sbt-lighter plugin that will automate creating and submitting this job to EMR
import sbtlighter._
import com.amazonaws.services.elasticmapreduce.model.Tag

sparkEmrRelease             := "emr-5.20.0"
sparkAwsRegion              := "us-east-1"
sparkEmrApplications        := Seq("Spark", "Zeppelin", "Ganglia")
sparkS3JarFolder            := "s3://wri-users/tmaschler/geotrellis-test/jars"
sparkS3LogUri               := Some("s3://wri-users/tmaschler/geotrellis-test/logs")
sparkSubnetId               := Some("subnet-116d9a4a")
sparkSecurityGroupIds       := Seq("sg-6c6a5911")
sparkInstanceCount := 20
sparkMasterType             := "m4.xlarge"
sparkCoreType               := "m4.xlarge"
sparkMasterEbsSize := Some(30)
sparkCoreEbsSize := Some(30)
sparkMasterPrice            := Some(0.5)
sparkCorePrice              := Some(0.5)
sparkClusterName            := s"geotrellis-treecoverloss"
sparkEmrServiceRole         := "EMR_DefaultRole"
sparkInstanceRole           := "EMR_EC2_DefaultRole"
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName("tmaschler_wri2")
sparkRunJobFlowRequest      := sparkRunJobFlowRequest.value.withTags(new Tag("Project", "Global Forest Watch"))
                                                      .withTags(new Tag("Job", "Annual Update Geotrellis"))
                                                      .withTags(new Tag("Project Lead", "Thomas Maschler"))
                                                      .withTags(new Tag("Name", "geotrellis-treecoverloss"))
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