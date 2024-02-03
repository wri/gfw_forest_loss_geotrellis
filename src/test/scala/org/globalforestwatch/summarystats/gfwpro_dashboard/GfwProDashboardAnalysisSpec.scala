package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.NonEmptyList
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import geotrellis.vector._
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType,BooleanType}
import org.globalforestwatch.features.{FeatureFilter, ValidatedFeatureRDD}
import org.globalforestwatch.summarystats.ValidatedLocation
import org.globalforestwatch.TestEnvironment
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.util.Config

class GfwProDashboardAnalysisSpec extends TestEnvironment with DataFrameComparer {
  def dashInputTsvPath = getClass.getResource("/dash.tsv").toString()
  def dashExpectedOutputPath = getClass.getResource("/dash-output").toString()
  // This is a partial version of gadm36_adm2_1_1.tsv that only includes IDN.1.5
  def idn1_5GadmTsvPath = getClass.getResource("/idn1_5Gadm.tsv").toString()

  def Dashboard(features: RDD[ValidatedLocation[Geometry]]) = {
    val fireAlertsRdd = {
      // I guess this is how they do things in Java?
      val spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = spark.sparkContext.emptyRDD[Geometry].toJavaRDD()
      spatialRDD
    }

    GfwProDashboardAnalysis(
      features,
      "gfwpro",
      "gadm",
      NonEmptyList.one(idn1_5GadmTsvPath),
      fireAlertsRdd,
      spark,
      kwargs = Map(
        "config" -> GfwConfig.get(Some(NonEmptyList.one(Config("gfw_integrated_alerts", "v20231121")))))
    )
  }

  /** Function to update expected results when this test becomes invalid */
  def saveExpectedFcdResult(fcd: DataFrame): Unit = {
    fcd.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(GfwProDashboardExport.csvOptions)
      .csv(path = dashExpectedOutputPath)
  }

  def readExpectedFcdResult = {
    val csvFile = spark.read
      .options(GfwProDashboardExport.csvOptions)
      .csv(dashExpectedOutputPath)
    // status_code and glad_alerts_coverage get interpreted as string type, so cast
    // them to their correct integer/boolean type.
    csvFile.withColumn("status_code", col("status_code").cast(IntegerType))
      .withColumn("glad_alerts_coverage", col("glad_alerts_coverage").cast(BooleanType))
  }

  it("matches recorded output for dashboard") {
    val featureLoc31RDD = ValidatedFeatureRDD(
      NonEmptyList.one(dashInputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true
    )
    val fcd = Dashboard(featureLoc31RDD)
    val summaryDF = GfwProDashboardDF.getFeatureDataFrameFromVerifiedRdd(fcd.unify, spark)
    //saveExpectedFcdResult(summaryDF)

    val expectedDF = readExpectedFcdResult

    assertSmallDataFrameEquality(summaryDF, expectedDF)
  }
}
