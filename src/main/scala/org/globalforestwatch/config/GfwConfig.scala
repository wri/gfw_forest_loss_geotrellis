package org.globalforestwatch.config

import scala.io.Source
import com.typesafe.scalalogging.LazyLogging

case class GfwConfig(rasterCatalog: RasterCatalog)

object GfwConfig {
  private val featureFlag: Option[String] = {
    val flag = scala.util.Properties.envOrNone("GFW_FEATURE_FLAG").map(_.toLowerCase())
    println(s"GFW_FEATURE_FLAG=$flag")
    flag
  }

  val get: GfwConfig = read(featureFlag.getOrElse("default"))

  def isGfwPro: Boolean = featureFlag == Some("pro")

  def read(flag: String): GfwConfig = {
    val rasterCatalogFile = s"raster-catalog-$flag.json"
    println(s"Reading $rasterCatalogFile")
    val rasterCatalog = RasterCatalog.getRasterCatalog(rasterCatalogFile)
    GfwConfig(rasterCatalog)
  }
}