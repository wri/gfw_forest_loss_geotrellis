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
  "GeoSolutions" at "https://maven.geo-solutions.it/",
  "LT-releases" at "https://repo.locationtech.org/content/groups/releases",
  "LT-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
  "OSGeo" at "https://repo.osgeo.org/repository/release/",
  "Open Source Geospatial Foundation Repository" at "https://repo.osgeo.org/repository/release/",
  "Apache Software Foundation Snapshots" at "https://repository.apache.org/content/groups/snapshots",
  "Java.net repository" at "https://download.java.net/maven/2",
  "Artifacts" at "https://mvnrepository.com/artifact",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "jitpack" at "https://jitpack.io",
  "maven2" at "https://repo1.maven.org/maven2",
  //  "Geotools Wrapper" at "https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper",
  "Geotools Metadata" at "https://mvnrepository.com/artifact/org.geotools/gt-metadata",
  Resolver.bintrayRepo("azavea", "geotrellis")
)


libraryDependencies ++= Seq(
  sparkCore,
  sparkSQL,
  sparkHive,
  "org.typelevel" %% "frameless-dataset" % Version.frameless,
  hadoopAws,
  hadoopCommon,
  hadoopMapReduceClientCore,
  logging,
  decline,
  paranamer,
  scalatest % Test,
  scalactic % Test,
  geotrellisSpark,
  geotrellisSparkTestKit % Test,
  geotrellisS3,
  geotrellisShapefile,
  geotrellisGeotools,
  geotrellisVectorTile,
  geotrellisGdal,
  logging,
  decline,
  sedonaCore,
  sedonaSQL,
  //  jtsCore,
  //  jts2geojson,
  geoToolsOGRBridj,
  bridj,
  breeze,
  breezeNatives,
  breezeViz,
  sparkDaria,
)

dependencyOverrides += "com.google.guava" % "guava" % "20.0"

assembly / assemblyShadeRules := {
  val shadePackage = "org.globalforestwatch.shaded"
  Seq(
    ShadeRule.rename("shapeless.**" -> s"$shadePackage.shapeless.@1").inAll,
    ShadeRule.rename("cats.kernel.**" -> s"$shadePackage.cats.kernel.@1").inAll
  )
}

// auto imports for local SBT console
// can be used with `test:console` command
initialCommands in console :=
  """
import java.net._
//import geotrellis.raster._
//import geotrellis.vector._
//import geotrellis.vector.io._
//import geotrellis.spark._
//import geotrellis.spark.tiling._
//import geotrellis.contrib.vlm._
//import geotrellis.contrib.vlm.gdal._
//import geotrellis.contrib.vlm.geotiff._
//import geotrellis.vector.io.wkt.WKT

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import org.globalforestwatch.summarystats.treecoverloss._
import org.globalforestwatch.util._

//
//val conf = new SparkConf().
//setAppName("Tree Cover Loss Console").
//set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
////.set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
//
//implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate
//implicit val sc: SparkContext = spark.sparkContext
"""

// settings for local testing
console / fork := true
Test / fork := true
Test / parallelExecution := false
Test / testOptions += Tests.Argument("-oD")
Test / javaOptions ++= Seq("-Xms1024m", "-Xmx8144m")
Test / envVars := Map("AWS_REQUEST_PAYER" -> "requester")

// Settings for sbt-assembly plugin which builds fat jars for use by spark jobs
test in assembly := {}
assemblyOption in assembly := (assemblyOption in assembly).value.copy(appendContentHash = true)
assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  // both GeoSpark and Geotrellis bring in this library, need to use GeoSpark version
  case PathList("org", "geotools", xs@_*) => MergeStrategy.first
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

sparkEmrRelease := "emr-6.1.0"
sparkAwsRegion := "us-east-1"
sparkEmrApplications := Seq("Spark", "Zeppelin", "Ganglia")
sparkS3JarFolder := "s3://gfw-pipelines-dev/geotrellis/jars"
sparkS3LogUri := Some("s3://gfw-pipelines-dev/geotrellis/logs")
sparkSubnetId := Some("subnet-067b91868bc3a8ff1")
sparkSecurityGroupIds := Seq("sg-07c11c0c9189c0a7a", "sg-068c422162d468700")
sparkInstanceCount := 21 // 201 for carbonflux and carbon_sensitivity
sparkMasterType := "r4.2xlarge"
sparkCoreType := "r4.2xlarge"
sparkMasterEbsSize := Some(10)
sparkCoreEbsSize := Some(10)
//sparkMasterPrice := Some(3.0320)
sparkCorePrice := Some(0.532)
sparkClusterName := s"geotrellis-forest-change-diagnostic"
sparkEmrServiceRole := "EMR_DefaultRole"
sparkInstanceRole := "EMR_EC2_DefaultRole"
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName(
  "tmaschler_gfw"
)
sparkEmrBootstrap := List(
  BootstrapAction(
    "Install GDAL 3.1.2 dependencies",
    "s3://gfw-pipelines/geotrellis/bootstrap/gdal.sh",
    "3.1.2"
  )
)
sparkRunJobFlowRequest := sparkRunJobFlowRequest.value
  .withTags(new Tag("Project", "Global Forest Watch"))
  .withTags(new Tag("Job", "Tree Cover Loss Analysis Geotrellis"))
  .withTags(new Tag("Project Lead", "Thomas Maschler"))
  .withTags(new Tag("Name", "geotrellis-treecoverloss"))
sparkEmrConfigs := List(
  // reference to example by geotrellis: https://github.com/geotrellis/geotrellis-spark-job.g8/blob/master/src/main/g8/build.sbt#L70-L91
  EmrConfig("spark").withProperties("maximizeResourceAllocation" -> "true"),
  EmrConfig("emrfs-site").withProperties("fs.s3.useRequesterPaysHeader" -> "true"),
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

//    "spark.dynamicAllocation.enabled" -> "false",
//    "spark.executor.cores" -> "1", //5",
//    "spark.executor.memory" -> "5652m", //37G
//    "spark.executor.memoryOverhead" -> "2g", //5G
//    "spark.driver.cores" -> "1",
//    "spark.driver.memory" -> "6652m",
//    "spark.executor.instances" -> "159", // 1599 for carbonflux and carbon_sensitivity
//    "spark.default.parallelism" -> "1590", // 15990 for carbonflux and carbon_sensitivity
//    "spark.sql.shuffle.partitions" -> "1590", // 15990 for carbonflux and carbon_sensitivity

    "spark.shuffle.spill.compress" -> "true",
    "spark.driver.maxResultSize" -> "3G",
    "spark.shuffle.compress" -> "true",
    "spark.yarn.appMasterEnv.LD_LIBRARY_PATH" -> "/usr/local/miniconda/lib/:/usr/local/lib",
    "spark.rdd.compress" -> "true",
    "spark.shuffle.service.enabled" -> "true",
    "spark.executorEnv.LD_LIBRARY_PATH" -> "/usr/local/miniconda/lib/:/usr/local/lib",
    "spark.dynamicAllocation.enabled" -> "true",

   // Use these GC strategy as default
   "spark.driver.defaultJavaOptions" -> "-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:OnOutOfMemoryError='kill -9 %p'",
   "spark.executor.defaultJavaOptions" -> "-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:OnOutOfMemoryError='kill -9 %p'",

    //    "spark.kryoserializer.buffer.max" -> "2047m",

    //     Best practice 4: Always set up a garbage collector when handling large volume of data through Spark.
    // Use these GC strategy to avoid java.lang.OutOfMemoryError: GC overhead limit exceeded
    // "spark.executor.defaultJavaOptions" -> "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
    // "spark.driver.defaultJavaOptions" -> "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",

    // set this environment variable for GDAL to use request payer method for S3 files
    "spark.yarn.appMasterEnv.AWS_REQUEST_PAYER" -> "requester",
    "spark.yarn.executorEnv.AWS_REQUEST_PAYER" -> "requester",

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
