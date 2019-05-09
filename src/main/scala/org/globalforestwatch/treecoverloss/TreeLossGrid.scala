package org.globalforestwatch.treecoverloss

import geotrellis.vector.Extent
import org.globalforestwatch.layers._
import org.globalforestwatch.util.TenByTenGrid

object TreeLossGrid extends TenByTenGrid {

  def getRasterSource(windowExtent: Extent): TreeLossGridSources = {
    val gridId = pointGridId(windowExtent.center)
    val sources = TreeLossGridSources(gridId)


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
    checkOptional(sources.biodiversitySignificance)
    checkOptional(sources.biodiversityIntactness)
    checkOptional(sources.protectedAreas)
    checkOptional(sources.aze)
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
