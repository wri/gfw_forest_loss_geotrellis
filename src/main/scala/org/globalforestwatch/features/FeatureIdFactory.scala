package org.globalforestwatch.features

case class FeatureIdFactory(featureType: String) {
  def featureId(id: Any): FeatureId =
    (featureType, id) match {
      case ("gadm", (iso: String, adm1: Integer, adm2: Integer)) =>
        GadmFeatureId(iso, adm1, adm2)
      case ("feature", simple_id: Int) => SimpleFeatureId(simple_id)
      case (
        "wdpa",
        (
          wdpaId: Int,
          name: String,
          iucnCat: String,
          iso: String,
          status: String
          )
        ) =>
        WdpaFeatureId(wdpaId, name, iucnCat, iso, status)
      case ("geostore", geostore: String) => GeostoreFeatureId(geostore)
      case ("burned_areas", alertDate: String) =>
        BurnedAreasFeatureId(alertDate)
      case (
        "viirs",
        (
          lon: Double,
          lat: Double,
          alertDate: String,
          alertTime: Int,
          confidence: String,
          brightTi4: Float,
          brightTi5: Float,
          frp: Float
          )
        ) =>
        FireAlertViirsFeatureId(
          lon,
          lat,
          alertDate,
          alertTime,
          confidence,
          brightTi4,
          brightTi5,
          frp
        )
      case (
        "modis",
        (
          lon: Double,
          lat: Double,
          alertDate: String,
          alertTime: Int,
          confidencePerc: Int,
          confidenceCat: String,
          brightness: Float,
          brightT31: Float,
          frp: Float
          )
        ) =>
        FireAlertModisFeatureId(
          lon,
          lat,
          alertDate,
          alertTime,
          confidencePerc,
          confidenceCat,
          brightness,
          brightT31,
          frp
        )
      case value =>
        throw new IllegalArgumentException(
          s"FeatureType must be one of 'gadm', 'wdpa', 'geostore', 'feature', 'viirs', 'modis', or 'burned_areas'. Got $value."
        )
    }

  def fromUserData(value: String, delimiter: String = "\t"): FeatureId = {

    val values = value.filterNot("[]".toSet).split(delimiter).map(_.trim)
    //    val values = value.drop(0).dropRight(1).split(delimiter).map(_.trim)

    featureType match {
      case "gadm" => GadmFeature.getFeatureId(values, true)
      case "feature" => SimpleFeatureId(values(0).toInt)
      case "wdpa" => WdpaFeature.getFeatureId(values, true)
      case "geostore" => GeostoreFeature.getFeatureId(values, true)
      case "burned_areas" => BurnedAreasFeature.getFeatureId(values, true)
      case "viirs" => FireAlertViirsFeature.getFeatureId(values, true)
      case "modis" => FireAlertModisFeature.getFeatureId(values, true)
      case value =>
        throw new IllegalArgumentException(
          s"FeatureType must be one of 'gadm', 'wdpa', 'geostore', 'feature', 'viirs', 'modis', or 'burned_areas'. Got $value."
        )
    }
  }

}
