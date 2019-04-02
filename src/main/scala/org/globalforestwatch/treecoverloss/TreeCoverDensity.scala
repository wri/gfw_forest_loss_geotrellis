package org.globalforestwatch.treecoverloss

object TreeCoverDensity {
  def threshold(density: Int): Int = {
    if (density <= 10) 0
    else if (density <= 15) 10
    else if (density <= 20) 15
    else if (density <= 25) 20
    else if (density <= 30) 25
    else if (density <= 50) 30
    else if (density <= 75) 50
    else 75
  }
}

object MexicoForestZoning {

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/mex_forest_zoning/${grid}.tif"

  def lookup(category: Int): (String, String, String) = category match {
    case 1 => ("I A", "Zonas de conservación y aprovechamiento restringido o prohibido", "Áreas Naturales Protegidas")
    case 2 => ("I C", "Zonas de conservación y aprovechamiento restringido o prohibido", "Áreas localizadas arriba de los tres mil metros sobre el nivel del mar")
    case 3 => ("I D", "Zonas de conservación y aprovechamiento restringido o prohibido", "Terrenos con pendientes mayores al cien por ciento o cuarenta y cinco grados")
    case 4 => ("I E", "Zonas de conservación y aprovechamiento restringido o prohibido", "Áreas cubiertas con vegetación de manglar o bosque mesófilo de montaña")
    case 5 => ("I F", "Zonas de conservación y aprovechamiento restringido o prohibido", "Áreas cubiertas con vegetación de galería")
    case 6 => ("I G", "Zonas de conservación y aprovechamiento restringido o prohibido", "Áreas cubiertas con selvas altas perennifolias")
    case 7 => ("I H", "Zonas de conservación y aprovechamiento restringido o prohibido", "*")
    case 8 => ("II A", "Zonas de producción", "Terrenos forestales de productividad alta")
    case 9 => ("II B", "Zonas de producción", "Terrenos forestales de productividad media")
    case 10 => ("II C", "Zonas de producción", "Terrenos forestales de productividad baja")
    case 11 => ("II D", "Zonas de producción", "Terrenos con vegetación forestal de zonas áridas")
    case 12 => ("II E", "Zonas de producción", "Terrenos adecuados para realizar forestaciones")
    case 13 => ("II F", "Zonas de producción", "Terrenos preferentemente forestales")
    case 14 => ("III A", "Zonas de restauración", "Terrenos forestales con degradación alta")
    case 15 => ("III B", "Zonas de restauración", "Terrenos preferentemente forestales con degradación alta")
    case 16 => ("III C", "Zonas de restauración", "Terrenos forestales o preferentemente forestales con degradación media")
    case 17 => ("III D", "Zonas de restauración", "Terrenos forestales o preferentemente forestales con degradación baja")
    case 18 => ("III E", "Zonas de restauración", "Terrenos forestales o preferentemente forestales degradados que se encuentran sometidos a tratamientos de recuperación forestal")
    case 19 => ("N/A", "No aplica", "No aplica")
  }
}

object PeruForestConcessions {

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/per_forest_concessions/${grid}.tif"

  def lookup(category:Int): String = category match{
    case 1 => "CONSERVATION"
    case 2 => "ECOTOURISM"
    case 3 => "NONTIMBER FOREST PRODUCTS_NUTS"
    case 4 => "NONTIMBER FOREST PRODUCTS_SHIRINGA"
    case 5 => "REFORESTATION"
    case 6 => "TIMBER CONCESSION"
    case 7 => "WILDLIFE"
  }
}
