package org.globalforestwatch.summarystats.afi

case class AFiDataGroup(
                         gadm_id: String
)

object AFiDataGroup {
      def empty: AFiDataGroup =
            AFiDataGroup("")
}