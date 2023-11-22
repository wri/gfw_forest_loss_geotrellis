package org.globalforestwatch.config

import cats.data.NonEmptyList
import org.globalforestwatch.util.Config

case class GfwConfig(rasterCatalog: RasterCatalog)

object GfwConfig {
  private val featureFlag: Option[String] = {
    val flag = scala.util.Properties.envOrNone("GFW_FEATURE_FLAG").map(_.toLowerCase())
    println(s"GFW_FEATURE_FLAG=$flag")
    flag
  }

  /** Read the configuration from the raster catalog. pinned specifies any entries using
   * 'latest' that should be pinned to a specified version, else use the actual
   * latest version of the dataset. */
  def get(pinned: Option[NonEmptyList[Config]]): GfwConfig = read(featureFlag.getOrElse("default"), pinned)

  def isGfwPro: Boolean = featureFlag == Some("pro")

  def read(flag: String, pinned: Option[NonEmptyList[Config]]): GfwConfig = {
    val rasterCatalogFile = s"raster-catalog-$flag.json"
    println(s"Reading $rasterCatalogFile")
    val rasterCatalog = RasterCatalog.getRasterCatalog(rasterCatalogFile, pinned)
    GfwConfig(rasterCatalog)
  }
}
