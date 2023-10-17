package org.globalforestwatch.summarystats

trait Summary[Self <: Summary[Self]] { self: Self =>
  def merge(other: Self): Self
  def isEmpty: Boolean
}
