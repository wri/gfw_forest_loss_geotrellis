package org.globalforestwatch.summarystats.afi

case class AFiDataGroup(
                         gadm_id: String,
                         loss_year: Integer
)

object AFiDataGroup {
      def empty: AFiDataGroup =
            AFiDataGroup("", 0)
}
