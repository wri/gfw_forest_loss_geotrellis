package usbuildings

import geotrellis.raster.histogram.StreamingHistogram

/** Properties for a feature in this application. */
case class FeatureProperties(
  featureId: FeatureId,
  histogram: Option[StreamingHistogram] = None
) {

  def merge(other: FeatureProperties): FeatureProperties = {
    require(other.featureId == featureId, s"ID mismatch: ${other.featureId} != $featureId")
    val hist = {
      val mergedHist = for (h1 <- histogram; h2 <- other.histogram) yield h1 merge h2
      mergedHist.orElse(histogram).orElse(other.histogram)
    }
    FeatureProperties(featureId, hist)
  }
}

/** Unique identifier for a feature.
  *
  * It is critical to keep track of feature identity both of output
  * and to reconcile partial results during job execution.
  *
  * @param state source state file
  * @param index index of feature in state FeatureCollection
  */
case class FeatureId(
  state: String,
  index: Int
)
