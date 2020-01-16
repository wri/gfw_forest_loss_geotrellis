import Dependencies._

name := "treecoverloss"
organization := "org.globalforestwatch"
licenses := Seq(
  "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")
)

scalaVersion := Version.scala
scalaVersion in ThisBuild := Version.scala

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
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
pomIncludeRepository := { _ =>
  false
}
addCompilerPlugin(
  "org.spire-math" % "kind-projector" % "0.9.4" cross CrossVersion.binary
)
addCompilerPlugin(
  "org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full
)
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
  logging,
  decline,
  scalatest % Test,
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

// spark-daria
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-daria" % "v0.35.0"

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

import org.globalforestwatch.summarystats.treecoverloss._
import org.globalforestwatch.util._


val conf = new SparkConf().
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
assemblyOption in assembly := (assemblyOption in assembly).value.copy(appendContentHash = true)
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
      case ("javax.media.jai.registryFile.jai" :: Nil) |
           ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
        MergeStrategy.concat
      case (name :: Nil) => {
        // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
        if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(
          ".SF"
        ))
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

//  Always check Spot prices for instance type and select subnet based on bested price
//  GFW subnet zone us-east-1a: subnet-00335589f5f424283
//  GFW subnet zone us-east-1b: subnet-8c2b5ea1
//  GFW subnet zone us-east-1c: subnet-08458452c1d05713b
//  GFW subnet zone us-east-1d: subnet-116d9a4a
//  GFW subnet zone us-east-1e: subnet-037b97cff4493e3a1
//  GFW subnet zone us-east-1f: subnet-0360516ee122586ff

sparkEmrRelease := "emr-5.27.0"
sparkAwsRegion := "us-east-1"
sparkEmrApplications := Seq("Spark", "Zeppelin", "Ganglia")
sparkS3JarFolder := "s3://gfw-files/2018_update/spark/jars"
sparkS3LogUri := Some("s3://gfw-files/2018_update/spark/logs")
sparkSubnetId := Some("subnet-0360516ee122586ff")
sparkSecurityGroupIds := Seq("sg-00ca15563a40c5687", "sg-6c6a5911")
sparkInstanceCount := 101
sparkMasterType := "r4.2xlarge"
sparkCoreType := "r4.2xlarge"
sparkMasterEbsSize := Some(10)
sparkCoreEbsSize := Some(10)
//sparkMasterPrice := Some(3.0320)
sparkCorePrice := Some(0.532)
sparkClusterName := s"geotrellis-treecoverloss"
sparkEmrServiceRole := "EMR_DefaultRole"
sparkInstanceRole := "EMR_EC2_DefaultRole"
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName(
  "tmaschler_wri2"
)
sparkRunJobFlowRequest := sparkRunJobFlowRequest.value
  .withTags(new Tag("Project", "Global Forest Watch"))
  .withTags(new Tag("Job", "Tree Cover Loss Analysis Geotrellis"))
  .withTags(new Tag("Project Lead", "Thomas Maschler"))
  .withTags(new Tag("Name", "geotrellis-treecoverloss"))
sparkEmrConfigs := List(
  EmrConfig("spark").withProperties("maximizeResourceAllocation" -> "true"),
  EmrConfig("spark-defaults").withProperties(
    // https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
    //    Best practice 1: Choose the right type of instance for each of the node types in an Amazon EMR cluster.
    //    Doing this is one key to success in running any Spark application on Amazon EMR.
    //
    //    Best practice 2: Set spark.dynamicAllocation.enabled to true only if the numbers are properly determined
    //    for spark.dynamicAllocation.initialExecutors/minExecutors/maxExecutors parameters. Otherwise,
    //    set spark.dynamicAllocation.enabled to false and control the driver memory, executor memory,
    //    and CPU parameters yourself. To do this, calculate and set these properties manually for each
    //    application (see the example following).

    // spark.executor.cores -> always 5 cCPU
    // Number of executors = (total number of virtual cores per instance - 1)/ spark.executors.cores //9
    // Total executor memory = total RAM per instance / number of executors per instance //42
    // spark.executors.memory -> total executor memory * 0.90
    // spark.yarn.executor.memoryOverhead -> total executor memory * 0.10
    // spark.driver.memory = spark.executors.memory
    // spark.driver.cores= spark.executors.cores
    // spark.executor.instances = (number of executors per instance * number of core instances) minus 1 for the driver
    // spark.default.parallelism = spark.executor.instances * spark.executors.cores * 2
    // spark.sql.shuffle.partitions = spark.default.parallelism
    "spark.dynamicAllocation.enabled" -> "false",
    "spark.executor.cores" -> "1", //5",
    "spark.executor.memory" -> "6652m", //37G
    "spark.executor.memoryOverhead" -> "1g", //5G
    "spark.driver.cores" -> "1",
    "spark.driver.memory" -> "6652m",
    "spark.executor.instances" -> "799", // 1339",
    "spark.default.parallelism" -> "7990", // "26790",
    "spark.sql.shuffle.partitions" -> "7990", //"26790",
    "spark.driver.maxResultSize" -> "3g",
    "spark.shuffle.service.enabled" -> "true",
    "spark.shuffle.compress" -> "true",
    "spark.shuffle.spill.compress" -> "true",
    "spark.rdd.compress" -> "true",
    //    "spark.kryoserializer.buffer.max" -> "2047m",

    //     Best practice 4: Always set up a garbage collector when handling large volume of data through Spark.

    // Use these GC strategy to avoid java.lang.OutOfMemoryError: GC overhead limit exceeded
    //    "spark.executor.extraJavaOptions" -> "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
    //    "spark.driver.extraJavaOptions" -> "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'"

    // Use these GC strategy as default
    "spark.driver.extraJavaOptions" -> "-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:OnOutOfMemoryError='kill -9 %p'",
    "spark.executor.extraJavaOptions" -> "-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:OnOutOfMemoryError='kill -9 %p'"

  ),
  //  EmrConfig("spark-env").withProperties(
  //    "LD_LIBRARY_PATH" -> "/usr/local/lib"
  //  ),
  EmrConfig("yarn-site").withProperties(
    //     Best practice 5: Always set the virtual and physical memory check flag to false.
    "yarn.resourcemanager.am.max-attempts" -> "1",
    "yarn.nodemanager.vmem-check-enabled" -> "false",
    "yarn.nodemanager.pmem-check-enabled" -> "false"
  )
)
