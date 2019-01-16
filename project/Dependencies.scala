/*
 * Copyright (c) 2014 Azavea.
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
  val geotrellis  = "2.2.0"
  val scala       = "2.11.12"
  val hadoop      = "2.8.0"
  val spark       = "2.3.1"
  val geotools    = "17.1"
  val gdal        = "2.3.0"
}

object Dependencies {
  val geotrellisSpark         = "org.locationtech.geotrellis" %% "geotrellis-spark"          % Version.geotrellis
  val geotrellisSparkTestKit  = "org.locationtech.geotrellis" %% "geotrellis-spark-testkit"  % Version.geotrellis
  val geotrellisS3            = "org.locationtech.geotrellis" %% "geotrellis-s3"             % Version.geotrellis
  val geotrellisRaster        = "org.locationtech.geotrellis" %% "geotrellis-raster"         % Version.geotrellis
  val geotrellisMacros        = "org.locationtech.geotrellis" %% "geotrellis-macros"         % Version.geotrellis
  val geotrellisRasterTestkit = "org.locationtech.geotrellis" %% "geotrellis-raster-testkit" % Version.geotrellis
  val geotrellisVector        = "org.locationtech.geotrellis" %% "geotrellis-vector"         % Version.geotrellis
  val geotrellisVectorTile    = "org.locationtech.geotrellis" %% "geotrellis-vectortile"     % Version.geotrellis
  val geotrellisUtil          = "org.locationtech.geotrellis" %% "geotrellis-util"           % Version.geotrellis
  val geotrellisShapefile     = "org.locationtech.geotrellis" %% "geotrellis-shapefile"      % Version.geotrellis
  val geotrellisGeotools      = "org.locationtech.geotrellis" %% "geotrellis-geotools"       % Version.geotrellis

  val pureconfig          = "com.github.pureconfig"      %% "pureconfig"               % "0.9.1"
  val logging             = "com.typesafe.scala-logging" %% "scala-logging"            % "3.9.0"
  val scalatest           = "org.scalatest"              %% "scalatest"                % "3.0.5"
  val scalactic           = "org.scalactic"              %% "scalactic"                % "3.0.5"
  val scalacheck          = "org.scalacheck"             %% "scalacheck"               % "1.14.0"
  val squants             = "org.typelevel"              %% "squants"                  % "1.3.0"
  val jts                 = "com.vividsolutions"          %  "jts-core"                % "1.14.0"

  val catsCore            = "org.typelevel"              %% "cats-core"                % "1.4.0"
  val catsEffect          = "org.typelevel"              %% "cats-effect"              % "1.0.0"
  val fs2Core             = "co.fs2"                     %% "fs2-core"                 % "1.0.0"
  val fs2Io               = "co.fs2"                     %% "fs2-io"                   % "1.0.0"

  val sparkCore           = "org.apache.spark"           %% "spark-core"               % Version.spark
  val sparkSQL            = "org.apache.spark"           %% "spark-sql"                % Version.spark
  val sparkHive           = "org.apache.spark"           %% "spark-hive"                % Version.spark
  val hadoopClient        = "org.apache.hadoop"           % "hadoop-client"            % Version.hadoop
  val gdal                = "org.gdal"                    % "gdal"                     % Version.gdal
}
