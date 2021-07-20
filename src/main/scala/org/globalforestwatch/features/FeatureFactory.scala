package org.globalforestwatch.features

case class FeatureFactory(featureName: String) {
  val featureObj: Feature = featureName match {
    case "gadm" => GadmFeature
    case "feature" => SimpleFeature
    case "wdpa" => WdpaFeature
    case "geostore" => GeostoreFeature
    case "viirs" => FireAlertViirsFeature
    case "modis" => FireAlertModisFeature
    case "burned_areas" => BurnedAreasFeature
    case "gfwpro" => GfwProFeature
    case value =>
      throw new IllegalArgumentException(
        s"FeatureType must be one of 'gadm', 'wdpa', 'geostore', 'gfwpro', 'feature', 'viirs', 'modis', or 'burned_areas'. Got $value."
      )
  }
}
