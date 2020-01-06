package org.globalforestwatch.features

case class FeatureFactory(featureName: String) {

  val featureObj: Feature = featureName match {
    case "gadm" => GadmFeature
    case "feature" => SimpleFeature
    case "wdpa" => WdpaFeature
    case "geostore" => GeostoreFeature
    case _ =>
      throw new IllegalArgumentException(
        "Feature type must be one of 'gadm', 'wdpa' 'geostore' and 'feature'"
      )
  }
}
