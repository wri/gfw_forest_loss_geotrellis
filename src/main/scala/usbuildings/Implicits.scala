package usbuildings

import cats._
import geotrellis.raster.histogram.StreamingHistogram

/** Here we define ad-hoc interface implementations.
  * These are interfaces required to perform polygonalSummary on a Raster[Tile]
  */
object Implicits {
  /** Instance of interface that defines how to:
    * - get an empty/initial value of StreamingHistogram
    * - combine two instances of StreamingHistogram
    *
    * ref: https://typelevel.org/cats/typeclasses/monoid.html
    */
  implicit val streamingHistogramMonoid: Monoid[StreamingHistogram] =
    new Monoid[StreamingHistogram] {
      def empty: StreamingHistogram = new StreamingHistogram(256)
      def combine(x: StreamingHistogram, y: StreamingHistogram): StreamingHistogram = x.merge(y)
    }
}
