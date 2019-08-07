package org.globalforestwatch.features

case class FeatureIdFactory(featureName: String) {

  featureName match {
    case "gadm"    => GadmFeatureId
    case "feature" => SimpleFeatureId
    case _         => throw IllegalArgumentException
  }

}
