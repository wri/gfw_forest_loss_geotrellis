package usbuildings

import cats._
import geotrellis.contrib.polygonal.CellAccumulator
import geotrellis.raster.histogram.{StreamingHistogram}

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
      override def empty: StreamingHistogram = new StreamingHistogram(256)
      override def combine(x: StreamingHistogram, y: StreamingHistogram): StreamingHistogram = x.merge(y)
    }

  /** Interface that defines how to register a single cell value with StreamingHistogram.
    */
  implicit val streamingHistogramCellAccumulator: CellAccumulator[StreamingHistogram] =
      new CellAccumulator[StreamingHistogram] {
      override def add(self: StreamingHistogram, v: Int): StreamingHistogram = {
        self.countItem(v, 1)
        self
      }

      override def add(self: StreamingHistogram, v: Double): StreamingHistogram = {
        self.countItem(v, 1)
        self
      }
    }
}
