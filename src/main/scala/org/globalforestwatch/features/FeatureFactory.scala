package org.globalforestwatch.features

case class FeatureFactory(featureName: String, fireAlertType: Option[String] = None) {
  val featureObj: Feature = featureName match {
    case "gadm" => GadmFeature
    case "feature" => SimpleFeature
    case "wdpa" => WdpaFeature
    case "geostore" => GeostoreFeature
    case "firealerts" => fireAlertType match {
      case Some("viirs") => FireAlertsViirsFeature
      case Some("modis") => FireAlertsModisFeature
      case Some("burned_areas") => BurnedAreasFeature
      case _ => throw new IllegalArgumentException("Cannot provide fire alert feature type without selecting alert" +
        "type 'modis' or 'viirs'")
    }
    case _ =>
      throw new IllegalArgumentException(
        "Feature type must be one of 'gadm', 'wdpa' 'geostore' and 'feature'"
      )
  }
}
