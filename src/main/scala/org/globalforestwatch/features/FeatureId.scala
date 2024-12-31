package org.globalforestwatch.features

trait FeatureId

object FeatureId {
  def fromUserData(featureType: String, value: String, delimiter: String = "\t"): FeatureId = {
    val values = value.filterNot("[]".toSet).split(delimiter).map(_.trim)

    featureType match {
      case "gadm"         => GadmFeature.getFeatureId(values, true)
      case "feature"      => SimpleFeatureId(values(0).toInt)
      case "wdpa"         => WdpaFeature.getFeatureId(values, true)
      case "geostore"     => GeostoreFeature.getFeatureId(values, true)
      case "gfwpro"       => GfwProFeature.getFeatureId(values, true)
      case "gfwpro_ext"   => GfwProFeatureExt.getFeatureId(values, true)
      case "burned_areas" => BurnedAreasFeature.getFeatureId(values, true)
      case "viirs"        => FireAlertViirsFeature.getFeatureId(values, true)
      case "modis"        => FireAlertModisFeature.getFeatureId(values, true)
      case value =>
        throw new IllegalArgumentException(
          s"FeatureType must be one of 'gadm', 'wdpa', 'geostore', 'feature', 'gfwpro', 'viirs', 'modis', or 'burned_areas'. Got $value."
        )
    }
  }
}
