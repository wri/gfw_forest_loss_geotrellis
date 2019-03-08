package workshop

import geotrellis.raster.TileLayout
import geotrellis.spark._
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT
import org.scalatest._

class LayoutDefinitionSpec extends FunSpec {

  /* LayoutDefinition represents a tiling of raster in a given projection */
  val layoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 360, layoutRows = 180,
      tileCols = 10812, tileRows = 10812)
    LayoutDefinition(worldExtent, tileLayout)
  }


  it("is able to give Extent to SpatialKey") {
    val keyExtent = SpatialKey(106,72).extent(layoutDefinition)
    info(s"$keyExtent")
  }

  it("is can tell which keys intersect a geometry") {
    val wkt = "POLYGON ((-73.175445 43.055058, -73.175373 43.055098, -73.175462 43.055181, -73.175534 43.055141, -73.175445 43.055058))"
    val geom = WKT.read(wkt)
    val keys = layoutDefinition.mapTransform.keysForGeometry(geom)
    info(s"$keys")
  }
}