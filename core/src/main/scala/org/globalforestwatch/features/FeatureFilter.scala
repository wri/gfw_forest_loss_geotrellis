package org.globalforestwatch.features

import org.globalforestwatch.summarystats.SummaryCommand
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Column

trait FeatureFilter {
  def filterConditions(): List[Column]
}

object FeatureFilter extends LazyLogging {
  def empty: FeatureFilter = new FeatureFilter {
    def filterConditions() = Nil
  }

  def fromOptions(featureType: String, options: SummaryCommand.AllFilterOptions): FeatureFilter = {
    val featureFilter = featureType match {
      case "gadm" => GadmFeature.Filter(options.base, options.gadm)
      case "feature" => SimpleFeature.Filter(options.base, options.featureId)
      case "wdpa" => WdpaFeature.Filter(options.base, options.gadm, options.wdpa)
      case "geostore" => FeatureFilter.empty
      case "viirs" => FeatureFilter.empty
      case "modis" => FeatureFilter.empty
      case "burned_areas" => FeatureFilter.empty
      case "gfwpro" => GfwProFeature.Filter(options.base, options.featureId)
      case value =>
        throw new IllegalArgumentException(
          s"FeatureType must be one of 'gadm', 'wdpa', 'geostore', 'gfwpro', 'feature', 'viirs', 'modis', or 'burned_areas'. Got $value."
        )
      }
    logger.info(s"Filter `$featureType` features with $featureFilter")
    featureFilter
  }
}
