package org.globalforestwatch.summarystats.afi

import cats.data.{NonEmptyList, Validated}
import com.github.mrpowers.spark.daria.sql.transformations.sortColumns
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import geotrellis.vector._
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.globalforestwatch.TestEnvironment
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features.{FeatureFilter, GfwProFeatureId, ValidatedFeatureRDD}
import org.globalforestwatch.summarystats.{JobError, Location, ValidatedLocation}

class AFiAnalysisSpec extends TestEnvironment with DataFrameComparer {
  def palm32InputTsvPath = getClass.getResource("/palm-oil-32.tsv").toString()
  def palm32ExpectedOutputPath = getClass.getResource("/palm-32-afi-output").toString()

  def AFI(features: RDD[ValidatedLocation[Geometry]]): DataFrame = {
    AFiAnalysis(
      features,
      "gfwpro",
      spark,
      kwargs = Map("config" -> GfwConfig.get())
    )
  }

  /** Function to update expected results when this test becomes invalid */
  def saveExpectedAFiResult(fcd: DataFrame): Unit = {
    fcd
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(AFiExport.csvOptions)
      .csv(path = palm32ExpectedOutputPath)
  }

  def readExpectedAFiResult = {
    spark.read
      .options(Map(
        "header" -> "true",
        "delimiter" -> "\t",
        "quote" -> "\u0000",
        "escape" -> "\u0000",
        "quoteMode" -> "NONE",
        "nullValue" -> "",
        "emptyValue" -> "",
        "inferSchema" -> "true"
      ))
      .csv(palm32ExpectedOutputPath)
      .withColumn("status_code", col("status_code").cast(IntegerType))
      .withColumn("gadm_id", lit(""))
      .withColumn("location_error", lit(""))
  }

  it("matches recorded output for palm oil mills location 31") {
    val featureLoc31RDD = ValidatedFeatureRDD(
      NonEmptyList.one(palm32InputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true
    ).filter {
      case Validated.Valid(Location(GfwProFeatureId(_, 31), _)) => true
      case _ => false
    }
    val afiDF = AFI(featureLoc31RDD)
    //saveExpectedAFiResult(afiDF)

    val expectedDF = readExpectedAFiResult
    assertSmallDataFrameEquality(afiDF.transform(sortColumns()), expectedDF.transform(sortColumns()), ignoreNullable = true)
  }
}
