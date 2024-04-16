package org.globalforestwatch.summarystats.annualupdate_minimal

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import cats.data.NonEmptyList
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.globalforestwatch.TestEnvironment
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features.{FeatureId, FeatureRDD, FeatureFilter}

class AnnualUpdateMinimalSpec extends TestEnvironment with DataFrameComparer {
  def idn1_5GadmInputTsvPath = getClass.getResource("/idn1_5Gadm.tsv").toString()
  def idn1_5GadmExpectedOutputPath = getClass.getResource("/idn1_5Gadm-aum-output").toString()

  def AnnualUpdateMinimal(features: RDD[Feature[Geometry, FeatureId]]): Unit = {
    AnnualUpdateMinimalAnalysis(
      features,
      "gadm",
      spark,
      kwargs = Map("config" -> GfwConfig.get())
    )
  }
  /** Function to update expected results when this test becomes invalid */
  def saveExpectedAumResult(aum: DataFrame, outputPath: String): Unit = {
    aum.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(AnnualUpdateMinimalExport.csvOptions)
      .csv(path = idn1_5GadmExpectedOutputPath)
  }

  def readExpectedFcdResult = {
    val csvFile = spark.read
      .options(AnnualUpdateMinimalExport.csvOptions)
      .csv(idn1_5GadmExpectedOutputPath)
  }

  it("matches recorded output for first partition") {
    val idn1_5GadmRDD = FeatureRDD(
      NonEmptyList.one(idn1_5GadmInputTsvPath), 
      "gadm", 
      filters = FeatureFilter.empty,
      splitFeatures = true,
      spark)
  }
}

// TODO: export dataframe, clean columns, compare with real results