package org.globalforestwatch.features

case class FeatureIdFactory(featureName: String) {
  def featureId = featureName match {
    case "gadm"    => GadmFeatureId
    case "feature" => SimpleFeatureId
    case "wdpa"    => WdpaFeatureId
    case "geostore" => GeostoreFeatureId
    case _ =>
      throw new IllegalArgumentException(
        "Feature type must be one of 'gadm', 'wdpa', 'geostore' or 'feature'"
      )
  }
}
