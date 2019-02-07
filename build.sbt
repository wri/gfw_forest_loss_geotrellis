import Dependencies._

name := "geotrellis-usbuildings"

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
dependencyUpdatesFilter := moduleFilter(organization = "org.scala-lang")
resolvers ++= Seq(
  "geosolutions" at "http://maven.geo-solutions.it/",
  "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
  "locationtech-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
  "osgeo" at "http://download.osgeo.org/webdav/geotools/",
   Resolver.bintrayRepo("azavea", "geotrellis")
)

libraryDependencies ++= Seq(
  sparkCore % Provided,
  sparkSQL % Provided,
  sparkHive % Provided,
  geotrellisSpark,
  geotrellisS3,
  geotrellisShapefile,
  geotrellisGeotools,
  geotrellisVectorTile,
  "org.geotools" % "gt-ogr-bridj" % Version.geotools
    exclude("com.nativelibs4java", "bridj"),
  "com.nativelibs4java" % "bridj" % "0.6.1",
  "com.azavea.geotrellis" %% "geotrellis-contrib-vlm" % "0.7.11-2.2",
  "com.monovore"  %% "decline" % "0.5.1"
)

// auto imports for local dev console
initialCommands in console :=
"""
import usbuildings._
import java.net._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.gdal._
"""

// settings for local testing
Test / fork := true
Test / parallelExecution := false
Test / testOptions += Tests.Argument("-oD")
Test / javaOptions ++= Seq("-Xms1024m", "-Xmx8144m", "-Djava.library.path=/usr/local/lib")

// Settings for sbt-assembly plugin which builds fat jars for spark-submit
assemblyMergeStrategy in assembly := {
  case s if s.startsWith("META-INF/services") => MergeStrategy.concat
  case "reference.conf" | "application.conf"  => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// Settings from sbt-lighter plugin that will automate creating and submitting this job to EMR
import sbtlighter._

sparkEmrRelease             := "emr-5.13.0"
sparkAwsRegion              := "us-east-1"
sparkEmrApplications        := Seq("Spark", "Zeppelin", "Ganglia")
sparkEmrBootstrap           := List(
  BootstrapAction(
    "GIS", "s3://geotrellis-test/eac/bootstrap-geopyspark.sh",
    "s3://geopyspark-resources/rpms/86d70ff7d21b74e2dfea6ec395a4564d713d44ee",
    "s3://geopyspark-resources/notebooks",
    "github",
    "LocalGitHubOAuthenticator",
    "51c3c12344d06e2b0850",
    "c7b608e3be66ae4332c88db21cae3f4a47313535",
    "s3://geopyspark-resources/jars/read-to-layout-improved-test.jar",
    "https://github.com/jbouffard/geopyspark/archive/cf381434a9d05f8286dea27c353df4b03f4e4fdd.zip"))
sparkS3JarFolder            := "s3://geotrellis-test/usbuildings/jars"
sparkInstanceCount          := 20
sparkMasterType             := "m4.xlarge"
sparkCoreType               := "m4.xlarge"
sparkMasterPrice            := Some(0.5)
sparkCorePrice              := Some(0.5)
sparkClusterName            := s"geotrellis-usbuildings"
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
    "spark.executor.extraJavaOptions" -> "-XX:+UseParallelGC -Dgeotrellis.s3.threads.rdd.write=64 -Djava.library.path=/usr/local/lib",
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
