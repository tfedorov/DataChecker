package com.tfedorov.datachecker.comparer

import scala.collection.immutable

protected[comparer] trait RowInfo {

  def originalSource: String

  def text: String
}

object RowInfo {

  final val MAINFRAME_SOURCE = "Mainframe"
  final val EMR_SOURCE = "EMR"

  val originalSourceCol: String = "originalSource"

  val textCol: String = "text"

  val sourceTextCols: immutable.Seq[String] = originalSourceCol :: textCol :: Nil
}