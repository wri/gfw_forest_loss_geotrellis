package org.globalforestwatch


import java.nio.file.{Files, Path}
import com.typesafe.scalalogging.Logger
import geotrellis.raster.testkit.RasterMatchers
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.globalforestwatch.summarystats.SummarySparkSession
import org.scalactic.Tolerance
import org.scalatest._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

trait TestEnvironment extends AnyFunSpec
  with Matchers with Inspectors with Tolerance with RasterMatchers {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  lazy val scratchDir: Path = {
    val outputDir = Files.createTempDirectory("gfw-scratch-")
    outputDir.toFile.deleteOnExit()
    outputDir
  }

  def sparkMaster = "local[*, 2]"

  implicit lazy val spark: SparkSession = SummarySparkSession(s"Test Session")
  implicit def sc: SparkContext = spark.sparkContext
}