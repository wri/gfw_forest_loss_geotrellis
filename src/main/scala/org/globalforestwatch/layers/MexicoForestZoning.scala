package org.globalforestwatch.layers

class MexicoForestZoning(grid: String) extends StringLayer with OptionalILayer {

  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/mex_forest_zoning/${grid}.tif"

  def lookup(value: Int): String = value match {
    case 1 =>
      "Áreas Naturales Protegidas" // ("I A", "Zonas de conservación y aprovechamiento restringido o prohibido", "Áreas Naturales Protegidas")
    case 2 =>
      "Áreas localizadas arriba de los tres mil metros sobre el nivel del mar" //("I C", "Zonas de conservación y aprovechamiento restringido o prohibido", "Áreas localizadas arriba de los tres mil metros sobre el nivel del mar")
    case 3 =>
      "Terrenos con pendientes mayores al cien por ciento o cuarenta y cinco grados" // ("I D", "Zonas de conservación y aprovechamiento restringido o prohibido", "Terrenos con pendientes mayores al cien por ciento o cuarenta y cinco grados")
    case 4 =>
      "Áreas cubiertas con vegetación de manglar o bosque mesófilo de montaña" //("I E", "Zonas de conservación y aprovechamiento restringido o prohibido", "Áreas cubiertas con vegetación de manglar o bosque mesófilo de montaña")
    case 5 =>
      "Áreas cubiertas con vegetación de galería" // ("I F", "Zonas de conservación y aprovechamiento restringido o prohibido", "Áreas cubiertas con vegetación de galería")
    case 6 =>
      "Áreas cubiertas con selvas altas perennifolias" // ("I G", "Zonas de conservación y aprovechamiento restringido o prohibido", "Áreas cubiertas con selvas altas perennifolias")
    case 7 =>
      "*" // ("I H", "Zonas de conservación y aprovechamiento restringido o prohibido", "*")
    case 8 =>
      "Terrenos forestales de productividad alta" // ("II A", "Zonas de producción", "Terrenos forestales de productividad alta")
    case 9 =>
      "Terrenos forestales de productividad media" // ("II B", "Zonas de producción", "Terrenos forestales de productividad media")
    case 10 =>
      "Terrenos forestales de productividad baja" // ("II C", "Zonas de producción", "Terrenos forestales de productividad baja")
    case 11 =>
      "Terrenos con vegetación forestal de zonas áridas" // ("II D", "Zonas de producción", "Terrenos con vegetación forestal de zonas áridas")
    case 12 =>
      "Terrenos adecuados para realizar forestaciones" // ("II E", "Zonas de producción", "Terrenos adecuados para realizar forestaciones")
    case 13 =>
      "Terrenos preferentemente forestales" // ("II F", "Zonas de producción", "Terrenos preferentemente forestales")
    case 14 =>
      "Terrenos forestales con degradación alta" // ("III A", "Zonas de restauración", "Terrenos forestales con degradación alta")
    case 15 =>
      "Terrenos preferentemente forestales con degradación alta" // ("III B", "Zonas de restauración", "Terrenos preferentemente forestales con degradación alta")
    case 16 =>
      "Terrenos forestales o preferentemente forestales con degradación media" // ("III C", "Zonas de restauración", "Terrenos forestales o preferentemente forestales con degradación media")
    case 17 =>
      "Terrenos forestales o preferentemente forestales con degradación baja" // ("III D", "Zonas de restauración", "Terrenos forestales o preferentemente forestales con degradación baja")
    case 18 =>
      "Terrenos forestales o preferentemente forestales degradados que se encuentran sometidos a tratamientos de recuperación forestal" // ("III E", "Zonas de restauración", "Terrenos forestales o preferentemente forestales degradados que se encuentran sometidos a tratamientos de recuperación forestal")
    case 19 => "No aplica" // ("N/A", "No aplica", "No aplica")
  }
}
