package org.globalforestwatch.treecoverloss


trait Layer{
  def source(grid: String): String
}


object BiodiversitySignificance extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/biodiversity_significance/${grid}.tif"
  def lookup(category:Float): String = category match {
    ???
  }
}


object BiomassPerHectar extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/biomass/${grid}.tif"
}

object BrazilBiomes extends Layer {

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/bra_biomes/${grid}.tif"

  def lookup(category:Int): String = category match {
    case 1 => "Caatinga"
    case 2 => "Cerrado"
    case 3 => "Pantanal"
    case 4 => "Pampa"
    case 5 => "Amazônia"
    case 6 => "Mata Atlântica"
  }
}


object Carbon extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/co2_pixel/${grid}.tif"
}

object Ecozones extends Layer {

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/ecozones/${grid}.tif"

  def lookup(category:Int): String = category match {
    case 1 => "Boreal coniferous forest"
    case 2 => "Boreal mountain system"
    case 3 => "Boreal tundra woodland"
    case 4 => "No data"
    case 5 => "Polar"
    case 6 => "Subtropical desert"
    case 7 => "Subtropical dry forest"
    case 8 => "Subtropical humid forest"
    case 9 => "Subtropical mountain system"
    case 10 => "Subtropical steppe"
    case 11 => "Temperate continental forest"
    case 12 => "Temperate desert"
    case 13 => "Temperate mountain system"
    case 14 => "Temperate oceanic forest"
    case 15 => "Temperate steppe"
    case 16 => "Tropical desert"
    case 17 => "Tropical dry forest"
    case 18 => "Tropical moist deciduous forest"
    case 19 => "Tropical mountain system"
    case 20 => "Tropical rainforest"
    case 21 => "Tropical shrubland"
    case 22 => "Water"
  }
}


object EndemicBirdAreas extends Layers {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/endemic_bird_areas/${grid}.tif"
}


object Erosion extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/erosion/${grid}.tif"

  def lookup(category:Int): String = category match {
    ???
  }
}


object GlobalLandcover extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/global_landcover/${grid}.tif"

  def lookup(category:Int): String = category match {
    ???
  }
}


object IndonesiaForestArea extends Layer {

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/idn_forest_area/${grid}.tif"

  def lookup(category:Int): (String, String, String) = category match {
    case 1 => ("Nature Reserve/Nature Conservation Area","Conservation Forest","Sanctuary Reserves/Nature Conservation Area")
    case 1001 => ("Protected Forest","Protected Forest","Protected Forest")
    case 1002 => ("Nature Reserve Forest","Conservation Forest","Sanctuary Reserves/Nature Conservation Area")
    case 1003 => ("Production Forest","Production Forest","Production Forest")
    case 1004 => ("Limited Production Forest","Production Forest","Limited Production Forest")
    case 1005=> ("Converted Production Forest","Production Forest","Converted Production Forest")
    case 1007 => ("Other Utilization Area","Other Utilization Area","Other Utilization Area")
    case 5001 => (null, null, null)
    case 5003 => (null, null, null)
    case 10021 => ("Sanctuary Reserve","Conservation Forest","Sanctuary Reserves/Nature Conservation Area")
    case 10022 => ("Wildlife Reserve","Conservation Forest","Sanctuary Reserves/Nature Conservation Area")
    case 10023 => ("Hunting Park","Conservation Forest","Sanctuary Reserves/Nature Conservation Area")
    case 10024 => ("National Park","Conservation Forest","Sanctuary Reserves/Nature Conservation Area")
    case 10025 => ("Nature Tourism Park","Conservation Forest","Sanctuary Reserves/Nature Conservation Area")
    case 10026 => ("Grand Forest Park","Conservation Forest","Sanctuary Reserves/Nature Conservation Area")
    case 100201 => ("Marine Nature Reserve/Nature Conservation Area","Conservation Forest","Marine Protected Areas")
    case 100211 => ("Marine Sanctuary Reserve","Conservation Forest","Marine Protected Areas")
    case 100221 => ("Marine Wildlife Reserve","Conservation Forest","Marine Protected Areas")
    case 100241 => ("Marine National Park","Conservation Forest","Marine Protected Areas")
    case 100251 => ("Marine Tourism Park","Conservation Forest","Marine Protected Areas")
  }
}


object IndonesiaForestMoratorium extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/idn_forest_moratorium/${grid}.tif"
}

object IndonesiaLandCover extends Layer {

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/idn_land_cover/${grid}.tif"

  def lookup(category:Int): String = category match {

    case 2001 => "Primary Dry Land Forest"
    case 2002 => "Secondary Dry Land Forest"
    case 2004 => "Primary Mangrove Forest"
    case 2005 => "Primary Swamp Forest"
    case 2006 => "Plantation Forest"
    case 2007 => "Bush / Shrub"
    case 2008 => null
    case 2010 => "Estate Crop Plantation"
    case 2011 => null
    case 2012 => "Settlement Area"
    case 2014 => "Bare Land"
    case 2020 => null
    case 2092 => null
    case 3000 => "Savannah"
    case 20021 => null
    case 20041 => "Secondary Mangrove Forest"
    case 20051 => "Secondary Swamp Forest"
    case 20071 => "Swamp Shrub"
    case 20091 => "Dryland Agriculture"
    case 20092 => "Shrub-Mixed Dryland Farm"
    case 20093 => "Rice Field"
    case 20094 => "Fish Pond"
    case 20102 => null
    case 20121 => "Airport	/ Harbour"
    case 20122 => "Transmigration Area"
    case 20141 => "Mining Area"
    case 20191 => null
    case 5001 => "Bodies of Water"
    case 50011 => "Swamp"
  }
}


object IndonesiaPrimaryForest extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/idn_primary_forest/${grid}.tif"
}


object IntactForestLandscapes extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/ifl/${grid}.tif"
}


object KeyBiodiversityAreas extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/kba/${grid}.tif"
}

object Landmark extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/landmark/${grid}.tif"
}

object LandRights extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/land_rights/${grid}.tif"
}

object Logging extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/logging/${grid}.tif"
}

object MangroveBiomass extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/mangrove_biomass/${grid}.tif"
}


object Mangroves1996 extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/mangroves_1996/${grid}.tif"
}


object Mangroves2016 extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/mangroves_2016/${grid}.tif"
}

object MexicoForestZoning extends Layer {

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


object MexicoPaymentForEcosystemServices extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/mex_psa/${grid}.tif"
}


object MexicoProtectedAreas extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/mex_protected_areas/${grid}.tif"
}


object Mining extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/mining/${grid}.tif"
}


object OilPalm extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/oil_palm/${grid}.tif"
}


object Peatlands extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/peatlands/${grid}.tif"
}


object PeruForestConcessions extends Layer {

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


object PeruProductionForest extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/per_permanent_production_forests/${grid}.tif"
}


object PeruProtectedAreas extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/per_protected_areas/${grid}.tif"
}

object Plantations extends Layer {

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/plantations/${grid}.tif"

  def lookup(category:Int): String = category match {
    case 1 => "Fruit"
    case 2 => "Fruit Mix"
    case 3 => "Oil Palm "
    case 4 => "Oil Palm Mix"
    case 5 => "Other"
    case 6 => "Rubber"
    case 7 => "Rubber Mix"
    case 8 => "Unknown"
    case 9 => "Unknown Mix"
    case 10 => "Wood fiber / timber"
    case 11 => "Wood fiber / timber Mix"
  }
}


object PrimaryForest extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/primary_forest/${grid}.tif"
}

object ProtectedAreas extends Layer{
  
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/wdpa/${grid}.tif"

  def lookup(category:Int): String = category match {
    case 1 => "Category Ia/b or II"
    case 2 => "Othe Category"
  }

}

object ResourceRights extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/resource_rights/${grid}.tif"
}

object RiverBasins extends Layer {

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/river_basins/${grid}.tif"

  def lookup(category:Int): String = category match {
    case 1001 => "Gulf of Mexico, North Atlantic Coast"
    case 1002 => "United States, North Atlantic Coast"
    case 1003 => "Mississippi - Missouri"
    case 1004 => "Gulf Coast"
    case 1005 => "California"
    case 1006 => "Great Basin"
    case 1007 => "North America, Colorado"
    case 1008 => "Columbia and Northwestern United States"
    case 1009 => "Fraser"
    case 1010 => "Pacific and Arctic Coast"
    case 1011 => "Saskatchewan - Nelson"
    case 1012 => "Northwest Territories"
    case 1013 => "Hudson Bay Coast"
    case 1014 => "Atlantic Ocean Seaboard"
    case 1015 => "Churchill"
    case 1016 => "St Lawrence"
    case 1017 => "St John"
    case 1018 => "Mackenzie"
    case 2001 => "Río Grande - Bravo"
    case 2002 => "Mexico, Northwest Coast"
    case 2003 => "Baja California"
    case 2004 => "Mexico, Interior"
    case 2005 => "North Gulf"
    case 2006 => "Río Verde"
    case 2007 => "Río Lerma"
    case 2008 => "Pacific Central Coast"
    case 2009 => "Río Balsas"
    case 2010 => "Papaloapan"
    case 2011 => "Isthmus of Tehuantepec"
    case 2012 => "Grijalva - Usumacinta"
    case 2013 => "Yucatán Peninsula"
    case 2014 => "Southern Central America"
    case 2015 => "Caribbean"
    case 3001 => "Caribbean Coast"
    case 3002 => "Magdalena"
    case 3003 => "Orinoco"
    case 3004 => "Northeast South America, South Atlantic Coast"
    case 3005 => "Amazon"
    case 3006 => "Tocantins"
    case 3007 => "North Brazil, South Atlantic Coast"
    case 3008 => "Parnaiba"
    case 3009 => "East Brazil, South Atlantic Coast"
    case 3010 => "Sao Francisco"
    case 3011 => "Uruguay - Brazil, South Atlantic Coast"
    case 3012 => "La Plata"
    case 3013 => "North Argentina, South Atlantic Coast"
    case 3014 => "South America, Colorado"
    case 3015 => "Negro"
    case 3016 => "South Argentina, South Atlantic Coast"
    case 3017 => "Central Patagonia Highlands"
    case 3018 => "Colombia - Ecuador, Pacific Coast"
    case 3019 => "Peru, Pacific Coast"
    case 3020 => "North Chile, Pacific Coast"
    case 3021 => "South Chile, Pacific Coast"
    case 3022 => "La Puna Region"
    case 3023 => "Salinas Grandes"
    case 3024 => "Mar Chiquita"
    case 3025 => "Pampas Region"
    case 4001 => "Spain - Portugal, Atlantic Coast"
    case 4002 => "Douro"
    case 4003 => "Tagus"
    case 4004 => "Guadiana"
    case 4005 => "Spain, South and East Coast"
    case 4006 => "Guadalquivir"
    case 4007 => "Ebro"
    case 4008 => "Gironde"
    case 4009 => "France, West Coast"
    case 4010 => "Loire"
    case 4011 => "Seine"
    case 4012 => "Rhône"
    case 4013 => "France, South Coast"
    case 4014 => "England and Wales"
    case 4015 => "Ireland"
    case 4016 => "Scotland"
    case 4017 => "Scheldt"
    case 4018 => "Rhine"
    case 4019 => "Maas"
    case 4020 => "Ems - Weser"
    case 4021 => "Po"
    case 4022 => "Italy, West Coast"
    case 4023 => "Tiber"
    case 4024 => "Italy, East Coast"
    case 4025 => "Danube"
    case 4026 => "Elbe"
    case 4027 => "Denmark - Germany Coast"
    case 4028 => "Sweden"
    case 4029 => "Wisla"
    case 4030 => "Oder"
    case 4031 => "Adriatic Sea - Greece - Black Sea Coast"
    case 4032 => "Dnieper"
    case 4033 => "Poland Coast"
    case 4034 => "Neman"
    case 4035 => "Dniester"
    case 4036 => "Don"
    case 4037 => "Volga"
    case 4038 => "Ural"
    case 4039 => "Daugava"
    case 4040 => "Narva"
    case 4042 => "Black Sea, North Coast"
    case 4043 => "Caspian Sea Coast"
    case 4044 => "Baltic Sea Coast"
    case 4045 => "Neva"
    case 4046 => "Mediterranean Sea Islands"
    case 4047 => "Scandinavia, North Coast"
    case 4048 => "Finland"
    case 4049 => "Russia, Barents Sea Coast"
    case 4050 => "Arctic Ocean Islands"
    case 4051 => "Northern Dvina"
    case 4052 => "Iceland"
    case 5001 => "Amur"
    case 5002 => "Bo Hai - Korean Bay, North Coast"
    case 5003 => "Russia, South East Coast"
    case 5004 => "Gobi Interior"
    case 5005 => "Ziya He, Interior"
    case 5006 => "Huang He"
    case 5007 => "Tarim Interior"
    case 5008 => "Plateau of Tibet Interior"
    case 5009 => "Yangtze"
    case 5010 => "China Coast"
    case 5011 => "Xun Jiang"
    case 5012 => "South China Sea Coast"
    case 5013 => "Kiribati - Nauru"
    case 5014 => "North and South Korea"
    case 5015 => "Andaman - Nicobar Islands"
    case 5016 => "Hong (Red River)"
    case 5017 => "Viet Nam, Coast"
    case 5018 => "Mekong"
    case 5019 => "Gulf of Thailand Coast"
    case 5020 => "Chao Phraya"
    case 5021 => "Peninsula Malaysia"
    case 5022 => "Salween"
    case 5023 => "Sittang"
    case 5024 => "Irrawaddy"
    case 5025 => "Bay of Bengal, North East Coast"
    case 5026 => "Hainan"
    case 5027 => "Sumatra"
    case 5028 => "Java - Timor"
    case 5029 => "Irian Jaya Coast"
    case 5030 => "Taiwan"
    case 5031 => "Sulawesi"
    case 5032 => "Kalimantan"
    case 5033 => "North Borneo Coast"
    case 5034 => "Philippines"
    case 5035 => "Ganges - Bramaputra"
    case 5036 => "Yasai"
    case 5037 => "Brahamani"
    case 5038 => "Mahandi"
    case 5039 => "India North East Coast"
    case 5040 => "Godavari"
    case 5041 => "Krishna"
    case 5042 => "Pennar"
    case 5043 => "India East Coast"
    case 5044 => "Cauvery"
    case 5045 => "India South Coast"
    case 5046 => "India West Coast"
    case 5047 => "Tapti"
    case 5048 => "Narmada"
    case 5049 => "Mahi"
    case 5050 => "Sabarmati"
    case 5051 => "Indus"
    case 5052 => "Sri Lanka"
    case 5053 => "Fly"
    case 5054 => "Papua New Guinea Coast"
    case 5055 => "Wake - Marshall Islands"
    case 5056 => "Japan"
    case 5057 => "Palau and East Indonesia"
    case 5058 => "North Marina Islands and Guam"
    case 5059 => "Sepik"
    case 5060 => "Micronesia"
    case 5061 => "Tuvalu"
    case 5062 => "Solomon Islands"
    case 5063 => "Lena"
    case 5064 => "Siberia, North Coast"
    case 5065 => "Yenisey"
    case 5066 => "Kara Sea Coast"
    case 5067 => "Ob"
    case 5068 => "Siberia, West Coast"
    case 6001 => "Black Sea, South Coast"
    case 6002 => "Mediterranean Sea, East Coast"
    case 6003 => "Caspian Sea, South West Coast"
    case 6004 => "Tigris - Euphrates"
    case 6005 => "Eastern Jordan - Syria"
    case 6006 => "Dead Sea"
    case 6007 => "Sinai Peninsula"
    case 6008 => "Red Sea, East Coast"
    case 6009 => "Arabian Peninsula"
    case 6010 => "Persian Gulf Coast"
    case 6011 => "Central Iran"
    case 6012 => "Arabian Sea Coast"
    case 6013 => "Hamun-i-Mashkel"
    case 6014 => "Helmand"
    case 6015 => "Farahrud"
    case 6016 => "Caspian Sea, East Coast"
    case 6017 => "Amu Darya"
    case 6018 => "Syr Darya"
    case 6019 => "Lake Balkash"
    case 7001 => "Senegal"
    case 7002 => "Niger"
    case 7003 => "Nile"
    case 7004 => "Shebelli - Juba"
    case 7005 => "Congo"
    case 7006 => "Zambezi"
    case 7007 => "Limpopo"
    case 7008 => "Orange"
    case 7009 => "Lake Chad"
    case 7010 => "Rift Valley"
    case 7011 => "Africa, South Interior"
    case 7014 => "Africa, North Interior"
    case 7015 => "Madasgacar"
    case 7016 => "South Africa, South Coast"
    case 7017 => "Africa, Indian Ocean Coast"
    case 7018 => "Africa, East Central Coast"
    case 7019 => "Africa, Red Sea - Gulf of Aden Coast"
    case 7020 => "Mediterranean South Coast"
    case 7021 => "Africa, West Coast"
    case 7022 => "Gulf of Guinea"
    case 7023 => "Angola, Coast"
    case 7024 => "South Africa, West Coast"
    case 7025 => "Namibia, Coast"
    case 7026 => "Africa, North West Coast"
    case 7027 => "Volta"
    case 8001 => "Australia, West Coast"
    case 8002 => "Australia, North Coast"
    case 8003 => "Australia, Interior"
    case 8004 => "Australia, South Coast"
    case 8005 => "Australia, East Coast"
    case 8006 => "Murray - Darling"
    case 8007 => "South Pacific Islands"
    case 8008 => "New Zealand"
    case 8009 => "Tasmania"
  }
}


object RSPO extends Layer{

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/rspo/${grid}.tif"

  def lookup(category:Int): String = category match {
    case 1 => "certified"
    case 2 => "unknown"
    case 3 => "not certified"
  }
}


object TigerLandscapes extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/tiger_landscapes/${grid}.tif"
}

trait TreeCoverDensity extends Layer{

  def lookup(density: Int): Int = {
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


object TreeCoverDensity2000 extends TreeCoverDensity {

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/tcd_2000/${grid}.tif"

}


object TreeCoverDensity2010 extends TreeCoverDensity {

  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/tcd_2000/${grid}.tif"

}


object TreeCoverGain extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/gain/${grid}.tif"
}

object TreeCoverLoss extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/loss/${grid}.tif"
}

object TreeCoverLossDrivers extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/drivers/${grid}.tif"

  def lookup(category:Int): String = category match {
    ???
  }
}


object UrbanWatersheds extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/urb_watersheds/${grid}.tif"
}

object WaterStress extends Layer{
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/water_stress/${grid}.tif"

  def lookup(category:Int): String = category match {
    case 1 => "Low risk"
    case 2 => "Low to medium risk"
    case 3 => "Medium to high risk"
    case 4 => "High risk"
    case 5 => "Extremely high risk"
  }
}


object WoodFiber extends Layer {
  def source(grid: String): String = s"s3://wri-users/tmaschler/prep_tiles/wood_fiber/${grid}.tif"
}