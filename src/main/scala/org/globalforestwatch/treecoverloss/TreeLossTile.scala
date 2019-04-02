package org.globalforestwatch.treecoverloss

import geotrellis.raster.{CellGrid, CellType, Tile}

/** Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  *
  * @param loss Tree Cover Loss Tile
  * @param gain Tree Cover Gain Tile
  * @param tcd2000 Tree Cover Density 2000 Tile
  * @param tcd2010 Tree Cover Density 2010 Tile
  * @param co2Pixel Tons of CO2 per Pixel Tile
  * @param gadm36 GADM v3.6 (admin level 2) Tile
  *
  */
case class TreeLossTile(
  loss: Tile,
  gain: Tile,
  tcd2000: Tile,
  tcd2010: Tile,
  co2Pixel: Tile,
  biomass: Tile,
  mangroveBiomass: Option[Tile],
  drivers: Option[Tile],
  globalLandCover: Option[Tile],
  primaryForest: Option[Tile],
  idnPrimaryForest: Option[Tile],
  erosion: Option[Tile],
  biodiversitySignificance: Option[Tile],
  wdpa: Option[Tile],
  plantations: Option[Tile],
  riverBasins: Option[Tile],
  ecozones: Option[Tile],
  urbanWatersheds: Option[Tile],
  mangroves1996: Option[Tile],
  mangroves2016: Option[Tile],
  waterStress: Option[Tile],
  intactForestLandscapes: Option[Tile],
  endemicBirdAreas: Option[Tile],
  tigerLandscapes: Option[Tile],
  landmark: Option[Tile],
  landRights: Option[Tile],
  keyBiodiversityAreas: Option[Tile],
  mining: Option[Tile],
  rspo: Option[Tile],
  peatlands: Option[Tile],
  oilPalm: Option[Tile],
  idnForestMoratorium: Option[Tile],
  idnLandCover: Option[Tile],
  mexProtectedAreas: Option[Tile],
  mexPaymentForEcosystemServices: Option[Tile],
  mexForestZoning: Option[Tile],
  perProductionForest: Option[Tile],
  perProtectedAreas: Option[Tile],
  perForestConcessions: Option[Tile],
  braBiomes: Option[Tile],
  woodFiber: Option[Tile],
  resourceRights: Option[Tile],
  logging: Option[Tile]
  
) extends CellGrid {
  def cellType: CellType = loss.cellType
  def cols: Int = loss.cols
  def rows: Int = loss.rows
}