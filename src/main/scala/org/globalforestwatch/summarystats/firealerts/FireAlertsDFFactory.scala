package org.globalforestwatch.summarystats.firealerts

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features._
import org.globalforestwatch.util.Util.getAnyMapValue

import scala.collection.immutable

case class FireAlertsDFFactory(
                                featureType: String,
                                summaryRDD: RDD[(FeatureId, FireAlertsSummary)],
                                spark: SparkSession,
                                kwargs: Map[String, Any]
) {
  val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")

  import spark.implicits._

  def getDataFrame: DataFrame = {
    fireAlertType match {
      case "viirs" => summaryRDD
        .flatMap {
          case (id, summary) =>
            summary.stats.map {
              case (dataGroup, data) => {
                id match {
                  case viirsId: FireAlertViirsFeatureId =>
                    FireAlertsRowViirs(viirsId, dataGroup, data)
                  case _ =>
                    throw new IllegalArgumentException("Not a valid Fire Alert ID")
                }
              }
            }
        }
        .toDF("fireId", "data_group", "data")
      case "modis" => summaryRDD
        .flatMap {
          case (id, summary) =>
            summary.stats.map {
              case (dataGroup, data) => {
                id match {
                  case modisId: FireAlertModisFeatureId =>
                    FireAlertsRowModis(modisId, dataGroup, data)
                  case _ =>
                    throw new IllegalArgumentException("Not a valid Fire Alert ID")
                }
              }
            }
        }
        .toDF("fireId", "data_group", "data")
      case "burned_areas" =>
        featureType match {
          case "gadm" => summaryRDD
            .flatMap {
              case (id, summary) =>
                summary.stats.map {
                  case (dataGroup, data) => {
                    id match {
                      case combinedId: CombinedFeatureId =>
                        combinedId match {
                          case CombinedFeatureId(gadmId: GadmFeatureId, burnedAreaId: BurnedAreasFeatureId) =>
                            BurnedAreasRowGadm(burnedAreaId, gadmId, dataGroup, data)
                          case _ =>
                            throw new IllegalArgumentException("Not a valid GADM-Burned Areas ID")
                        }
                      case _ =>
                        throw new IllegalArgumentException("Not a valid Fire Alert ID")
                    }
                  }
                }
            }
            .toDF("fireId", "featureId", "data_group", "data")
          case "wdpa" => summaryRDD
            .flatMap {
              case (id, summary) =>
                summary.stats.map {
                  case (dataGroup, data) => {
                    id match {
                      case combinedId: CombinedFeatureId =>
                        combinedId match {
                          case CombinedFeatureId(wdpaId: WdpaFeatureId, burnedAreaId: BurnedAreasFeatureId) =>
                            BurnedAreasRowWdpa(burnedAreaId, wdpaId, dataGroup, data)
                          case _ =>
                            throw new IllegalArgumentException("Not a valid WDPA-Burned Areas ID")
                        }
                      case _ =>
                        throw new IllegalArgumentException("Not a valid Fire Alert ID")
                    }
                  }
                }
            }
            .toDF("fireId", "featureId", "data_group", "data")
          case "geostore" => summaryRDD
            .flatMap {
              case (id, summary) =>
                summary.stats.map {
                  case (dataGroup, data) => {
                    id match {
                      case combinedId: CombinedFeatureId =>
                        combinedId match {
                          case CombinedFeatureId(geostoreId: GeostoreFeatureId, burnedAreaId: BurnedAreasFeatureId) =>
                            BurnedAreasRowGeostore(burnedAreaId, geostoreId, dataGroup, data)
                          case _ =>
                            throw new IllegalArgumentException("Not a valid Geostore-Burned Areas ID")
                        }
                      case _ =>
                        throw new IllegalArgumentException("Not a valid Fire Alert ID")
                    }
                  }
                }
            }
            .toDF("fireId", "featureId", "data_group", "data")
        }
    }
  }
}
