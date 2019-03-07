package usbuildings

case class Id(areaType: String, country: String, admin1: Int, admin2: Int)

case class TreeLossRecord(id: Id, summary: TreeLossSummary) {
  def merge(other: TreeLossRecord): TreeLossRecord = {
    require(other.id == id, s"${other.id} != ${id}")

    TreeLossRecord(id, TreeLossSummary())
  }
}
