package org.globalforestwatch.treecoverloss

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{TenByTenGrid, GridId}

object TreeLossGrid extends TenByTenGrid[TreeLossGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String) = TreeLossGridSources(gridId)

  def checkSources(gridId: String,
                   windowExtent: Extent): TreeLossGridSources = {

    val sources: TreeLossGridSources = getSources(gridId)

    checkRequired(sources.treeCoverLoss, windowExtent)
    checkRequired(sources.treeCoverGain, windowExtent)
    checkRequired(sources.treeCoverDensity2000, windowExtent)
    checkRequired(sources.treeCoverDensity2010, windowExtent)
    checkRequired(sources.biomassPerHectar, windowExtent)

    checkOptional(sources.mangroveBiomass, windowExtent)
    checkOptional(sources.treeCoverLossDrivers, windowExtent)
    checkOptional(sources.globalLandCover, windowExtent)
    checkOptional(sources.primaryForest, windowExtent)
    checkOptional(sources.indonesiaPrimaryForest, windowExtent)
    checkOptional(sources.erosion, windowExtent)
    checkOptional(sources.biodiversitySignificance, windowExtent)
    checkOptional(sources.biodiversityIntactness, windowExtent)
    checkOptional(sources.protectedAreas, windowExtent)
    checkOptional(sources.aze, windowExtent)
    checkOptional(sources.plantations, windowExtent)
    checkOptional(sources.riverBasins, windowExtent)
    checkOptional(sources.ecozones, windowExtent)
    checkOptional(sources.urbanWatersheds, windowExtent)
    checkOptional(sources.mangroves1996, windowExtent)
    checkOptional(sources.mangroves2016, windowExtent)
    checkOptional(sources.waterStress, windowExtent)
    checkOptional(sources.intactForestLandscapes, windowExtent)
    checkOptional(sources.endemicBirdAreas, windowExtent)
    checkOptional(sources.tigerLandscapes, windowExtent)
    checkOptional(sources.landmark, windowExtent)
    checkOptional(sources.landRights, windowExtent)
    checkOptional(sources.keyBiodiversityAreas, windowExtent)
    checkOptional(sources.mining, windowExtent)
    checkOptional(sources.rspo, windowExtent)
    checkOptional(sources.peatlands, windowExtent)
    checkOptional(sources.oilPalm, windowExtent)
    checkOptional(sources.indonesiaForestMoratorium, windowExtent)
    checkOptional(sources.indonesiaLandCover, windowExtent)
    checkOptional(sources.indonesiaForestArea, windowExtent)
    checkOptional(sources.mexicoProtectedAreas, windowExtent)
    checkOptional(sources.mexicoPaymentForEcosystemServices, windowExtent)
    checkOptional(sources.mexicoForestZoning, windowExtent)
    checkOptional(sources.peruProductionForest, windowExtent)
    checkOptional(sources.peruProtectedAreas, windowExtent)
    checkOptional(sources.peruForestConcessions, windowExtent)
    checkOptional(sources.brazilBiomes, windowExtent)
    checkOptional(sources.woodFiber, windowExtent)
    checkOptional(sources.resourceRights, windowExtent)
    checkOptional(sources.logging, windowExtent)
    checkOptional(sources.oilGas, windowExtent)
    sources

  }

}
