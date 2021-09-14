package org.globalforestwatch.config

import pureconfig.generic.auto._
import pureconfig.ConfigSource
import com.typesafe.scalalogging.LazyLogging

case class GfwConfig(
  rasterLayers: Map[String, String]
)

object GfwConfig extends LazyLogging {
  private val featureFlag: Option[String] = {
    val flag = scala.util.Properties.envOrNone("GFW_FEATURE_FLAG").map(_.toLowerCase())
    logger.info(s"GFW_FEATURE_FLAG=$flag")
    flag
  }

  def isGfwPro: Boolean = featureFlag == Some("pro")

  lazy val get: GfwConfig = read(featureFlag.getOrElse("pro"))

  def read(flag: String): GfwConfig = {
    val confFile = s"feature-flag-$flag.conf"
    logger.info(s"Reading $confFile")
    ConfigSource.resources(confFile).loadOrThrow[GfwConfig]
  }
}