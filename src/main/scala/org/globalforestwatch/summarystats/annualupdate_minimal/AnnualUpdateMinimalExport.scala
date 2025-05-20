package org.globalforestwatch.summarystats.annualupdate_minimal

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

object AnnualUpdateMinimalExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    val changeOnly: Boolean =
      getAnyMapValue[Boolean](kwargs, "changeOnly")

    val spark: SparkSession = summaryDF.sparkSession
    import spark.implicits._

    val exportDF = summaryDF
      .transform(
        AnnualUpdateMinimalDF.unpackValues(
          List($"id.iso" as "iso", $"id.adm1" as "adm1", $"id.adm2" as "adm2")
        )
      )

    exportDF.cache()
    if (!changeOnly) {
      exportWhitelist(exportDF, outputUrl)
      exportSummary(exportDF, outputUrl)
      exportDownload(exportDF, outputUrl)
    }
    exportChange(exportDF, outputUrl)
    exportDF.unpersist()

  }

  private def exportWhitelist(df: DataFrame, outputUrl: String): Unit = {

    val adm2ApiDF =
      df.transform(AnnualUpdateMinimalDF.whitelist(List("iso", "adm1", "adm2")))
    adm2ApiDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/whitelist")

    val adm1ApiDF =
      adm2ApiDF.transform(AnnualUpdateMinimalDF.whitelist2(List("iso", "adm1")))
    adm1ApiDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/whitelist")

    val isoApiDF =
      adm1ApiDF.transform(AnnualUpdateMinimalDF.whitelist2(List("iso")))
    isoApiDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/whitelist")

  }

  private def exportSummary(df: DataFrame, outputUrl: String): Unit = {

    val adm2ApiDF = df.transform(
      AnnualUpdateMinimalDF.aggSummary(List("iso", "adm1", "adm2"))
    )
    adm2ApiDF
      .coalesce(40) // this should result in an avg file size of 100MB
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/summary")

    val adm1ApiDF = adm2ApiDF.transform(
      AnnualUpdateMinimalDF.aggSummary2(List("iso", "adm1"))
    )
    adm1ApiDF
      .coalesce(12) // this should result in an avg file size of 100MB
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/summary")

    val isoApiDF =
      adm1ApiDF.transform(AnnualUpdateMinimalDF.aggSummary2(List("iso")))
    isoApiDF
      .coalesce(3) // this should result in an avg file size of 100MB
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/summary")

  }

  private def exportChange(df: DataFrame, outputUrl: String): Unit = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val adm2ApiDF = df
      .filter($"umd_tree_cover_loss__year".isNotNull &&
        ($"umd_tree_cover_loss__ha" > 0 || $"gfw_full_extent_gross_emissions__Mg_CO2e" > 0))
      .transform(AnnualUpdateMinimalDF.aggChange(List("iso", "adm1", "adm2")))
      .coalesce(65) // this should result in an avg file size of 100MB

    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/change")

    val adm1ApiDF = adm2ApiDF
      .transform(AnnualUpdateMinimalDF.aggChange(List("iso", "adm1")))
      .coalesce(35) // this should result in an avg file size of 100MB

    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/change")

    val isoApiDF = adm1ApiDF
      .transform(AnnualUpdateMinimalDF.aggChange(List("iso")))
      .coalesce(14) // this should result in an avg file size of 100MB

    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/change")
  }

  private def exportDownload(df: DataFrame, outputUrl: String): Unit = {

    val spark = df.sparkSession
    import spark.implicits._

    val adm2SummaryDF = df
      .transform(AnnualUpdateMinimalDownloadDF.sumDownload)

    adm2SummaryDF
      .transform(
        AnnualUpdateMinimalDownloadDF.roundDownload(
          List(
            $"iso" as "country",
            $"adm1" as "subnational1",
            $"adm2" as "subnational2"
          )
        )
      )
      .coalesce(1)
      .orderBy(
        $"country",
        $"subnational1",
        $"subnational2",
        $"umd_tree_cover_density_2000__threshold"
      )
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/download")

    val adm1SummaryDF =
      adm2SummaryDF.transform(
        AnnualUpdateMinimalDownloadDF.sumDownload(List("iso", "adm1"))
      )

    adm1SummaryDF
      .transform(
        AnnualUpdateMinimalDownloadDF
          .roundDownload2(List($"iso" as "country", $"adm1" as "subnational1"))
      )
      .coalesce(1)
      .orderBy($"country", $"subnational1", $"umd_tree_cover_density_2000__threshold")
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/download")

    val isoSummaryDF =
      adm1SummaryDF.transform(
        AnnualUpdateMinimalDownloadDF.sumDownload2(List("iso"))
      )

    isoSummaryDF
      .transform(
        AnnualUpdateMinimalDownloadDF
          .roundDownload2(List($"iso" as "country"))
      )
      .coalesce(1)
      .orderBy($"country", $"umd_tree_cover_density_2000__threshold")
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/download")

  }

  override protected def exportWdpa(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    val spark: SparkSession = summaryDF.sparkSession
    import spark.implicits._

    val idCols: List[String] = List(
      "wdpa_protected_area__id",
      "wdpa_protected_area__name",
      "wdpa_protected_area__iucn_cat",
      "wdpa_protected_area__iso",
      "wdpa_protected_area__status"
    )

    val changeOnly: Boolean =
      getAnyMapValue[Boolean](kwargs, "changeOnly")

    val exportDF = summaryDF
      .transform(
        AnnualUpdateMinimalDF.unpackValues(
          List(
            $"id.wdpaId" as "wdpa_protected_area__id",
            $"id.name" as "wdpa_protected_area__name",
            $"id.iucnCat" as "wdpa_protected_area__iucn_cat",
            $"id.iso" as "wdpa_protected_area__iso",
            $"id.status" as "wdpa_protected_area__status"
          ),
          wdpa = true
        )
      )

    exportDF.cache()
    if (!changeOnly) {
      exportDF
        .transform(AnnualUpdateMinimalDF.whitelist(idCols, wdpa = true))
        .coalesce(1)
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/wdpa/whitelist")

      exportDF
        .transform(AnnualUpdateMinimalDF.aggSummary(idCols, wdpa = true))
        .coalesce(33) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/wdpa/summary")
    }
    exportDF
      .filter($"umd_tree_cover_loss__year".isNotNull && $"umd_tree_cover_loss__ha" > 0)
      .transform(AnnualUpdateMinimalDF.aggChange(idCols, wdpa = true))
      .coalesce(50) // this should result in an avg file size of 100MB
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/wdpa/change")

    exportDF.unpersist()
  }

  override protected def exportGeostore(summaryDF: DataFrame,
                                        outputUrl: String,
                                        kwargs: Map[String, Any]): Unit = {

    val spark: SparkSession = summaryDF.sparkSession
    import spark.implicits._

    val idCols: List[String] = List("geostore__id")

    val changeOnly: Boolean = getAnyMapValue[Boolean](kwargs, "changeOnly")

    val exportDF = summaryDF
      .transform(
        AnnualUpdateMinimalDF
          .unpackValues(List($"id.geostoreId" as "geostore__id"))
      )

    exportDF.cache()
    if (!changeOnly) {

      exportDF
        .transform(AnnualUpdateMinimalDF.whitelist(idCols))
        .coalesce(1)
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/geostore/whitelist")

      exportDF
        .transform(AnnualUpdateMinimalDF.aggSummary(idCols))
        .coalesce(10) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/geostore/summary")
    }
    exportDF
      .filter($"umd_tree_cover_loss__year".isNotNull && $"umd_tree_cover_loss__ha" > 0)
      .transform(AnnualUpdateMinimalDF.aggChange(idCols))
      .coalesce(30) // this should result in an avg file size of 100MB
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/geostore/change")

    exportDF.unpersist()
  }
}
