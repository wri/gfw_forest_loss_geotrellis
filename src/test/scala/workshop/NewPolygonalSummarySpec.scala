/*package workshop

import cats._
import geotrellis.contrib.polygonal._
import geotrellis.contrib.vlm.geotiff._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT
import org.scalatest._

/* application specific accumulator for polygonal summary */
case class MyMean(total: Double, count: Long) {
  def result: Double = total / count.toDouble
}

/**
 * Contained example of how to add application specific result type for polygonal summary
 */
object MyMean {
  /** Specifies how to register each cell under polygon with MyMean */
  implicit val myMeanIsCellVisitor: CellVisitor[Raster[Tile], MyMean] = {
    new CellVisitor[Raster[Tile], MyMean] {
      def register(raster: Raster[Tile], col: Int, row: Int, acc: MyMean): MyMean = {
        val v = raster.tile.getDouble(col, row)
        if (isData(v)) {
          MyMean(acc.total + v, acc.count + 1)
        } else {
          acc
        }
      }
    }
  }

  /** Specified empty value for MyMean and how it can be merged
    *
    * */
  implicit val myMeanIsMonoid: Monoid[MyMean] =
    new Monoid[MyMean] {
      def empty: MyMean = MyMean(0, 0L)
      def combine(x: MyMean, y: MyMean): MyMean =
        MyMean(x.total + y.total, x.count + y.count)
    }
}

class NewSummarySpec extends FunSpec {
  val rasterSource = GeoTiffRasterSource("s3://gfw2-data/forest_change/hansen_2018/50N_080W.tif")
  val raster: Raster[Tile] = rasterSource.read(Extent(-72.97754892, 43.85921846, -72.80676639, 43.97153490)).get.mapTile(_.band(0))

  val nonIntersectingWkt = "POLYGON ((-73.175445 43.055058, -73.175373 43.055098, -73.175462 43.055181, -73.175534 43.055141, -73.175445 43.055058))"
  val nonIntersectingGeom: Geometry = WKT.read(nonIntersectingWkt)

  it("will perform a summary (non-intersecting)") {
    val mymean = raster.polygonalSummary[MyMean](nonIntersectingGeom)
    info(s"Result: $mymean")
  }

  val intersectingGeom = raster.extent.toPolygon
  it("will perform a summary (intersecting)") {

    val mymean: MyMean = raster.polygonalSummary[MyMean](intersectingGeom, MyMean(0, 0))
    info(s"raster cols: ${raster.cols} rows: ${raster.rows}")
    info(s"Result: $mymean")
  }

}*/