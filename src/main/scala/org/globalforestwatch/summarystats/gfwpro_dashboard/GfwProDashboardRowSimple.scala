package org.globalforestwatch.summarystats.gfwpro_dashboard

case class GfwProDashboardRowSimple(listId: String,
                                    locationId: String,
                                    gadmId: String,
                                    gladAlertsCoverage: String,
                                    gladAlertsDaily: String,
                                    gladAlertsWeekly: String,
                                    gladAlertsMonthly: String,
                                    viirsAlertsDaily: String)
