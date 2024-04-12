package org.globalforestwatch.summarystats.annualupdate_minimal

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.globalforestwatch.TestEnvironment
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features.FeatureId

class AnnualUpdateMinimalSpec extends TestEnvironment with DataFrameComparer {
  def palm32InputTsvPath = getClass.getResource("/palm-oil-32.tsv").toString()
  def palm32ExpectedOutputPath = getClass.getResource("/palm-32-afi-output").toString()

  def AnnualUpdateMinimal(features: RDD[Feature[Geometry, FeatureId]]): Unit = {
    AnnualUpdateMinimalAnalysis(
      features,
      "gadm",
      spark,
      kwargs = Map("config" -> GfwConfig.get())
    )
  }

  def saveExpectedAumResult(aum: DataFrame, outputPath: String): Unit = {
    aum.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(AnnualUpdateMinimalExport.csvOptions)
      .csv(path = outputPath)
  }
}

// TODO: replace with a gadm geom, export dataframe, clean columns, compare with real results