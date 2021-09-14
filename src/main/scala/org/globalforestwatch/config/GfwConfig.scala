package org.globalforestwatch.config

import pureconfig.generic.auto._
import pureconfig.ConfigSource
import com.typesafe.scalalogging.LazyLogging

case class GfwConfig(
  rasterLayers: Map[String, String]
)

object GfwConfig extends LazyLogging {
  private var featureFlag: String = null
  def isGfwPro: Boolean = featureFlag == "pro"

  def setProFlag(isPro: Boolean) = {
    require(featureFlag == null, "Pro feature flag already set")
    if (isPro) featureFlag = "pro"
    else featureFlag = "flagship"
  }

  lazy val get: GfwConfig = {
    if (featureFlag == null) featureFlag = "flagship"
    read(featureFlag)
  }

  def read(flag: String): GfwConfig = {
    val confFile = s"feature-flag-$flag.conf"
    logger.info(s"Reading $confFile")
    ConfigSource.resources(confFile).loadOrThrow[GfwConfig]
  }
}