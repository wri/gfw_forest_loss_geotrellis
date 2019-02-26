package usbuildings

import geotrellis.raster.histogram.StreamingHistogram

case class FeatureId(
  state: String,
  index: Int
)

case class FeatureProperties(
  featureId: FeatureId,
  histogram: StreamingHistogram
)