package org.globalforestwatch.summarystats.annualupdate_minimal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, GadmFeatureId, GeostoreFeatureId, WdpaFeatureId}

case class AnnualUpdateMinimalDFFactory(
  featureType: String,
  summaryRDD: RDD[(FeatureId, AnnualUpdateMinimalSummary)],
  spark: SparkSession
) {
  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "gadm"     => getGadmDataFrame
      case "geostore" => getGeostoreDataFrame
      case "wdpa" => getWdpaDataFrame
      case _ =>
        throw new IllegalArgumentException("Not a valid FeatureId")
    }
  }

  private def getGadmDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case gadmId: GadmFeatureId =>
                  AnnualUpdateMinimalRowGadm(gadmId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a GadmFeatureId")
              }
            }
          }
      }
      .toDF("id", "data_group", "data")
  }

  private def getGeostoreDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case geostoreId: GeostoreFeatureId =>
                  AnnualUpdateMinimalRowGeostore(geostoreId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a GeostoreFeatureId")
              }
            }
          }
      }
      .toDF("id", "data_group", "data")
  }

  private def getWdpaDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case wdpaId: WdpaFeatureId =>
                  AnnualUpdateMinimalRowWdpa(wdpaId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a WdpaFeatureId")
              }
            }
          }
      }
      .toDF("id", "data_group", "data")
  }
}
