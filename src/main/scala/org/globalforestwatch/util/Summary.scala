package org.globalforestwatch.util

trait Summary[Self <: Summary[Self]] { self: Self =>
  def merge(other: Self): Self
}
