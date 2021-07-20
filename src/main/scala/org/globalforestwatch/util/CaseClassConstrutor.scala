package org.globalforestwatch.util

import scala.reflect.ClassTag

object CaseClassConstrutor {

  def createCaseClassFromMap[T](vals: Map[String, Object])(implicit cmf: ClassTag[T]): T = {
    val ctor = cmf.runtimeClass.getConstructors.head
    val args = cmf.runtimeClass.getDeclaredFields.map(f => vals(f.getName))
    ctor.newInstance(args: _*).asInstanceOf[T]
  }
}
