/*
 * Copyright (c) 2019 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._

object Version {
  val breeze = "0.13.2"
  val decline = "1.3.0"
  val scala = "2.12.12"
  val geotrellis = "3.7.0"
  val hadoop = "3.2.1"
  val jackson = "2.10.0"
  val spark = "3.3.2"
  val sparkCompatible = "3.0"
  val geotools = "24.1"
  val sedona = "1.2.0-incubating"
  val frameless = "0.13.0"
  val jts = "1.19.0"
}

object Dependencies {

  private val dependencyScope = "provided"

  val sparkJts = "org.locationtech.geomesa" %% "geomesa-spark-jts" % "2.3.1"
  val jts = "org.locationtech.jts" % "jts-core" % Version.jts
  val geotrellisSpark = "org.locationtech.geotrellis" %% "geotrellis-spark" % Version.geotrellis
  val geotrellisS3 = "org.locationtech.geotrellis" %% "geotrellis-s3" % Version.geotrellis
  val geotrellisRaster = "org.locationtech.geotrellis" %% "geotrellis-raster" % Version.geotrellis
  val paranamer = "com.thoughtworks.paranamer" % "paranamer" % "2.8"
  val geotrellisVector = "org.locationtech.geotrellis" %% "geotrellis-vector" % Version.geotrellis
  val geotrellisVectorTile = "org.locationtech.geotrellis" %% "geotrellis-vectortile" % Version.geotrellis
  val geotrellisUtil = "org.locationtech.geotrellis" %% "geotrellis-util" % Version.geotrellis
  val geotrellisShapefile = "org.locationtech.geotrellis" %% "geotrellis-shapefile" % Version.geotrellis
  val geotrellisGeotools = "org.locationtech.geotrellis" %% "geotrellis-geotools" % Version.geotrellis
  val geotrellisSparkTestKit = "org.locationtech.geotrellis" %% "geotrellis-spark-testkit" % Version.geotrellis
  val geotrellisRasterTestkit = "org.locationtech.geotrellis" %% "geotrellis-raster-testkit" % Version.geotrellis
  val geotrellisGdal = "org.locationtech.geotrellis" %% "geotrellis-gdal" % Version.geotrellis

  // 3.7.x warp bindings corresponds to gdal 3.7.x; this will not necessarily match `Version.geotrellis`
  val geotrellisGdalWarp = "com.azavea.geotrellis" % "gdal-warp-bindings" % "3.7.0"

  val pureconfig = "com.github.pureconfig" %% "pureconfig" % "0.9.1"
  val logging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.8"
  val scalactic = "org.scalactic" %% "scalactic" % "3.0.8"
  val decline = "com.monovore" %% "decline" % Version.decline

  val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % Version.jackson % dependencyScope
  val jacksonCore = "com.fasterxml.jackson.core" % "jackson-core" % Version.jackson % dependencyScope

  val sparkCore = "org.apache.spark" %% "spark-core" % Version.spark % dependencyScope exclude("org.apache.hadoop", "*")
  val sparkSQL = "org.apache.spark" %% "spark-sql" % Version.spark % dependencyScope exclude("org.apache.hadoop", "*")
  val sparkHive = "org.apache.spark" %% "spark-hive" % Version.spark % dependencyScope exclude("org.apache.hadoop", "*")
  val frameless = "org.typelevel" %% "frameless-dataset" % Version.frameless

  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % Version.hadoop % dependencyScope
  val hadoopMapReduceClientCore = "org.apache.hadoop" % "hadoop-mapreduce-client-core" % Version.hadoop % dependencyScope
  val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % Version.hadoop % dependencyScope
  val hadoopAws = "org.apache.hadoop" % "hadoop-aws" % Version.hadoop % dependencyScope

  //  val sedonaCore = "org.apache.sedona" %% "sedona-core-".concat(
  //    Version.sparkCompatible
  //  ) % Version.sedona
  //  val sedonaSQL = "org.apache.sedona" %% "sedona-sql-".concat(
  //    Version.sparkCompatible
  //  ) % Version.sedona

  val sedonaCore = "org.apache.sedona" %% "sedona-core-3.0" % Version.sedona
  val sedonaSQL = "org.apache.sedona" %% "sedona-sql-3.0" % Version.sedona

  //  val jts2geojson = "org.wololo" % "jts2geojson" % "0.14.3" % "compile"
  //  val jtsCore = "org.locationtech.jts" % "jts-core" % "1.16.1" % "compile"  // 1.18.0 ?
  val geoToolsOGRBridj = "org.geotools" % "gt-ogr-bridj" % Version.geotools exclude("com.nativelibs4java", "bridj")
  val bridj = "com.nativelibs4java" % "bridj" % "0.6.1"
  val breeze = "org.scalanlp" %% "breeze" % Version.breeze
  val breezeNatives = "org.scalanlp" %% "breeze-natives" % Version.breeze
  val breezeViz = "org.scalanlp" %% "breeze-viz" % Version.breeze
  val sparkDaria = "com.github.mrpowers" % "spark-daria_2.12" % "0.38.2"
  val sparkFastTests = "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0"
}
