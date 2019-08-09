package org.globalforestwatch.features

case class FeatureIdFactory(featureName: String) {
  def featureId = featureName match {
    case "gadm"    => GadmFeatureId
    case "feature" => SimpleFeatureId
    case "wdpa"    => WdpaFeatureId
    case _ =>
      throw new IllegalArgumentException(
        "Feature type must be one of 'gadm' and 'feature'"
      )
  }
}
