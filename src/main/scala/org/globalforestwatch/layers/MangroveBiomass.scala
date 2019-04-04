package org.globalforestwatch.layers

class MangroveBiomass(grid: String) extends DoubleLayer with OptionalDLayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/mangrove_biomass/${grid}.tif"
}
