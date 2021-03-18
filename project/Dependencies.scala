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
  val scala = "2.12.12"
  val geotrellis = "3.5.0"
  val hadoop = "3.2.1"
  val spark = "3.0.0"
  val sparkCompatible = "3.0"
  val geotools = "23.1"
  val sedona = "1.0.0-incubating"
}

object Dependencies {
  val geotrellisSpark         = "org.locationtech.geotrellis" %% "geotrellis-spark"          % Version.geotrellis
  val geotrellisS3            = "org.locationtech.geotrellis" %% "geotrellis-s3"             % Version.geotrellis
  val geotrellisRaster        = "org.locationtech.geotrellis" %% "geotrellis-raster"         % Version.geotrellis
  val geotrellisVector = "org.locationtech.geotrellis" %% "geotrellis-vector" % Version.geotrellis
  val geotrellisVectorTile = "org.locationtech.geotrellis" %% "geotrellis-vectortile" % Version.geotrellis
  val geotrellisUtil = "org.locationtech.geotrellis" %% "geotrellis-util" % Version.geotrellis
  val geotrellisShapefile = "org.locationtech.geotrellis" %% "geotrellis-shapefile" % Version.geotrellis
  val geotrellisGeotools = "org.locationtech.geotrellis" %% "geotrellis-geotools" % Version.geotrellis
  val geotrellisSparkTestKit = "org.locationtech.geotrellis" %% "geotrellis-spark-testkit" % Version.geotrellis
  val geotrellisRasterTestkit = "org.locationtech.geotrellis" %% "geotrellis-raster-testkit" % Version.geotrellis
  val geotrellisGdal = "org.locationtech.geotrellis" %% "geotrellis-gdal" % Version.geotrellis
  val sparkJts = "org.locationtech.geomesa" %% "geomesa-spark-jts" % "2.2.0"

  val sedonaCore = "org.apache.sedona" %% "sedona-core-".concat(Version.sparkCompatible) % Version.sedona
  val sedonaSQL = "org.apache.sedona" %% "sedona-sql-".concat(Version.sparkCompatible) % Version.sedona

  val pureconfig = "com.github.pureconfig" %% "pureconfig" % "0.9.1"
  val logging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.5"
  val scalactic = "org.scalactic" %% "scalactic" % "3.0.5"
  val decline = "com.monovore" %% "decline" % "0.5.1"

  val sparkCore = "org.apache.spark" %% "spark-core" % Version.spark
  val sparkSQL = "org.apache.spark" %% "spark-sql" % Version.spark
  val sparkHive = "org.apache.spark" %% "spark-hive" % Version.spark
  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % Version.hadoop
  val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % Version.hadoop
  val hadoopAws = "org.apache.hadoop" % "hadoop-aws" % Version.hadoop

  val jtsCore = "org.locationtech.jts" % "jts-core" % "1.18.0" % "compile"
}
