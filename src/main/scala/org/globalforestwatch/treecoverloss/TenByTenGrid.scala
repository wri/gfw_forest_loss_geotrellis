package org.globalforestwatch.treecoverloss

import geotrellis.vector.Point
import geotrellis.raster.TileLayout
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.Extent
import org.globalforestwatch.layers._

object TenByTenGrid {

  /** this represents the tile layout of 10x10 degrees */
  val rasterFileGrid: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 36, layoutRows = 18,
      tileCols = 40000, tileRows = 40000)
    LayoutDefinition(worldExtent, tileLayout)
  }

  /** Windows inside each 10x10 tile for distributing the job IO
    * Matched to read GeoTiffs written with striped segment layout
    */
  val stripedTileGrid: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 36, layoutRows = 18 * (40000 / 10),
      tileCols = 40000, tileRows = 10)
    LayoutDefinition(worldExtent, tileLayout)
  }
  /** Windows inside each 10x10 tile for distributing the job IO
    * Matched to read GeoTiffs written with tiled segment layout
    */
  val blockTileGrid: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 3600, layoutRows = 1800,
      tileCols = 400, tileRows = 400)
    LayoutDefinition(worldExtent, tileLayout)
  }

  /** Translate from a point on a map to file grid ID of 10x10 grid
    * Top-Left corner, exclusive on south, east, inclusive on north and west
    */
  def pointGridId(point: Point): String = {
    val col = math.floor(point.x / 10).toInt * 10
    val long: String = if (col >= 0) f"$col%03dE" else f"${-col}%03dW"

    val row = math.ceil(point.y / 10).toInt * 10
    val lat: String = if (row >= 0) f"$row%02dN" else f"${-row}%02dS"

    s"${lat}_$long"
  }

  def getRasterSource(windowExtent: Extent): TenByTenGridSources = {
    val gridId = pointGridId(windowExtent.center)
    val sources = TenByTenGridSources(gridId)


    // NOTE: This check will cause an eager fetch of raster metadata
    def checkRequired(layer: RequiredLayer): Unit = {
      require(layer.source.extent.intersects(windowExtent),
        s"${layer.uri} does not intersect: $windowExtent")
    }

    // Only check these guys if they're defined
    def checkOptional(layer: OptionalLayer): Unit = {
      layer.source.foreach { source =>
        require(source.extent.intersects(windowExtent),
          s"${source.uri} does not intersect: $windowExtent")
      }
    }

    checkRequired(sources.treeCoverLoss)
    checkRequired(sources.treeCoverGain)
    checkRequired(sources.treeCoverDensity2000)
    checkRequired(sources.treeCoverDensity2010)
    checkRequired(sources.biomassPerHectar)

    checkOptional(sources.mangroveBiomass)
    checkOptional(sources.treeCoverLossDrivers)
    checkOptional(sources.globalLandCover)
    checkOptional(sources.primaryForest)
    checkOptional(sources.indonesiaPrimaryForest)
    checkOptional(sources.erosion)
    //checkOptional(sources.biodiversitySignificance)
    //checkOptional(sources.biodiversityIntactness)
    checkOptional(sources.protectedAreas)
    checkOptional(sources.plantations)
    checkOptional(sources.riverBasins)
    checkOptional(sources.ecozones)
    checkOptional(sources.urbanWatersheds)
    checkOptional(sources.mangroves1996)
    checkOptional(sources.mangroves2016)
    checkOptional(sources.waterStress)
    checkOptional(sources.intactForestLandscapes)
    checkOptional(sources.endemicBirdAreas)
    checkOptional(sources.tigerLandscapes)
    checkOptional(sources.landmark)
    checkOptional(sources.landRights)
    checkOptional(sources.keyBiodiversityAreas)
    checkOptional(sources.mining)
    checkOptional(sources.rspo)
    checkOptional(sources.peatlands)
    checkOptional(sources.oilPalm)
    checkOptional(sources.indonesiaForestMoratorium)
    checkOptional(sources.indonesiaLandCover)
    checkOptional(sources.indonesiaForestArea)
    checkOptional(sources.mexicoProtectedAreas)
    checkOptional(sources.mexicoPaymentForEcosystemServices)
    checkOptional(sources.mexicoForestZoning)
    checkOptional(sources.peruProductionForest)
    checkOptional(sources.peruProtectedAreas)
    checkOptional(sources.peruForestConcessions)
    checkOptional(sources.brazilBiomes)
    checkOptional(sources.woodFiber)
    checkOptional(sources.resourceRights)
    checkOptional(sources.logging)
    checkOptional(sources.oilGas)
    sources
  }
}
