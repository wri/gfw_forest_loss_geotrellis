package org.globalforestwatch.layers

class GlobalLandcover(grid: String) extends StringLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/global_landcover/${grid}.tif"

  def lookup(value: Int): String = value match {
    case 10 | 11 | 12 | 20 | 30 | 40 => "Agriculture"
    case 50 | 60 | 61 | 62 | 70 | 71 | 72 | 80 | 81 | 82 | 90 | 100 | 160 |
        170 =>
      "Forest"
    case 110 | 130                   => "Grassland"
    case 180                         => "Wetland"
    case 190                         => "Settlement"
    case 120 | 121 | 122             => "Shrubland"
    case 140 | 150 | 151 | 152 | 153 => "Sparse vegetation"
    case 200 | 201 | 202             => "Bare"
    case 210                         => "Water"
    case 220                         => "Permanent snow and ice"

  }
}
