package org.globalforestwatch.config

import scala.io.Source
import com.typesafe.scalalogging.LazyLogging

case class GfwConfig(rasterCatalog: RasterCatalog)

object GfwConfig extends LazyLogging {
  private val featureFlag: Option[String] = {
    val flag = scala.util.Properties.envOrNone("GFW_FEATURE_FLAG").map(_.toLowerCase())
    logger.info(s"GFW_FEATURE_FLAG=$flag")
    flag
  }

  def isGfwPro: Boolean = featureFlag == Some("pro")

  val get: GfwConfig = read(featureFlag.getOrElse("default"))

  def read(flag: String): GfwConfig = {
    val rasterCatalogFile = s"raster-catalog-$flag.json"
    logger.info(s"Reading $rasterCatalogFile")
    val rasterCatalog = RasterCatalog.getRasterCatalog(rasterCatalogFile)
    GfwConfig(rasterCatalog)
  }
}