package org.globalforestwatch.summarystats.annualupdate_minimal

case class AnnualUpdateMinimalDataGroup(lossYear: Integer,
                                        threshold: Integer,
                                        primaryForest: Boolean,
                                        wdpa: String,
                                        aze: Boolean,
                                        plantedForests: String,
                                        landmark: Boolean,
                                        keyBiodiversityAreas: Boolean,
                                        peatlands: Boolean,
                                        idnForestMoratorium: Boolean,
                                        intactForestLandscapes2000: Boolean,
                                        naturalForests: String
                                       )
