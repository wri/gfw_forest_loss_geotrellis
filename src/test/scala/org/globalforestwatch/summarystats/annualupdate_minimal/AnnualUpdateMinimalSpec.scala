package org.globalforestwatch.summarystats.annualupdate_minimal

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.rdd.RDD
import cats.data.NonEmptyList
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.globalforestwatch.TestEnvironment
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features.{FeatureId, FeatureRDD, FeatureFilter}

class AnnualUpdateMinimalSpec extends TestEnvironment with DataFrameComparer {
  def idn1_5GadmInputTsvPath = getClass.getResource("/idn1_5Gadm.tsv").toString()
  def idn1_5GadmExpectedOutputPath = getClass.getResource("/idn1_5Gadm-aum-output").toString()
  def wdpaInputTsvPath = getClass.getResource("/mulanje.tsv").toString()
  
  
  val csvOptions: Map[String, String] = Map(
  "header" -> "true",
  "delimiter" -> "\t",
  "escape" -> "\"",
  "quoteMode" -> "MINIMAL",
  "nullValue" -> "",
  "emptyValue" -> ""
)

  def AnnualUpdateMinimal(inputPath: String, featureType: String): DataFrame = {
    
    // Create featureRDD
    val featureRDD = FeatureRDD(
      NonEmptyList.one(inputPath), 
      featureType, 
      FeatureFilter.empty,
      splitFeatures = true,
      spark)

    // Run AnnualUpdateMinimal Analysis
    val kwargs = Map("config" -> GfwConfig.get())
    val aumRDD: RDD[(FeatureId, AnnualUpdateMinimalSummary)] = 
      AnnualUpdateMinimalRDD(featureRDD, AnnualUpdateMinimalGrid.blockTileGrid, kwargs)
    val aumDFFactory = AnnualUpdateMinimalDFFactory(featureType, aumRDD, spark)
    val aumDF: DataFrame = aumDFFactory.getDataFrame

    aumDF
  }

  /** Function to update expected results when this test becomes invalid */
  def saveExpectedAumResult(aum: DataFrame, outputPath: String): Unit = {
    aum.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(AnnualUpdateMinimalExport.csvOptions)
      .csv(path = idn1_5GadmExpectedOutputPath)
  }
  
  def readAumResult(path: String) = {
    spark.read
      .options(Map(
        "header" -> "true",
        "delimiter" -> "\t",
        "escape" -> "\"",
        "quoteMode" -> "MINIMAL",
        "nullValue" -> null,
        "emptyValue" -> null,
        "inferSchema" -> "true"
      ))
      .csv(path)
  }

  it("matches recorded output for GADM") {
    val gadmDF: DataFrame = AnnualUpdateMinimal(idn1_5GadmInputTsvPath, "gadm")

    // Export and save results 
    import spark.implicits._
    val exportDF = gadmDF      
      .transform(
        AnnualUpdateMinimalDF.unpackValues(
          List($"id.iso" as "iso", $"id.adm1" as "adm1", $"id.adm2" as "adm2")
        )
      )
    val adm2DF: DataFrame = exportDF.transform(
      AnnualUpdateMinimalDF.aggSummary(List("iso", "adm1", "adm2"))
    ) 
    val top20Rows = adm2DF.limit(20)
    top20Rows
      .write
      .options(csvOptions)
      .csv("output/idntest")

    // Read expected results and compare
    val expectedDF = readAumResult(idn1_5GadmExpectedOutputPath)
    val top20RowsDF = readAumResult("output/idntest")
    top20RowsDF.show()
    expectedDF.show()

    assertSmallDataFrameEquality(top20RowsDF, expectedDF)

  }

  it("matches recorded output for WDPA") {
    val wdpaDF: DataFrame = AnnualUpdateMinimal(idn1_5GadmInputTsvPath, "wdpa")

  }

}

// TODO: export dataframe, clean columns, compare with real results