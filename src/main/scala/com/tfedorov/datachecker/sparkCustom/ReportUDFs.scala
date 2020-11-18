package com.tfedorov.datachecker.sparkCustom

import com.tfedorov.datachecker.comparer.RowInfo
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{collect_list, struct, udf}
import org.apache.spark.sql.{Column, ColumnName}

import scala.collection.mutable

object ReportUDFs {

  def colExtractUDF(column: String): UserDefinedFunction = {
    udf { cell: GenericRowWithSchema =>
      val map: Map[String, String] = cell.getValuesMap[String](column :: Nil)
      map.head._2
    }
  }

  def cols2List(columns2Collect: Seq[String]): Column = {
    val cols = columns2Collect.map(new ColumnName(_))
    collect_list(struct(cols: _*))
  }

  def matchedUDF: UserDefinedFunction = {
    udf { cell: mutable.WrappedArray[_] =>
      val sources = cell.flatMap(el => el.asInstanceOf[GenericRowWithSchema].getValuesMap[String](RowInfo.originalSourceCol :: Nil).values)
      val emrs = sources.count("Mainframe".equalsIgnoreCase)
      val mainFrames = sources.count("EMR".equalsIgnoreCase)

      math.min(emrs, mainFrames)
    }
  }

  def unMatchedUDF: UserDefinedFunction = {
    udf { cell: mutable.WrappedArray[_] =>
      val sources = cell.flatMap(el => el.asInstanceOf[GenericRowWithSchema].getValuesMap[String](RowInfo.originalSourceCol :: Nil).values)
      val emrs = sources.count("Mainframe".equalsIgnoreCase)
      val mainFrames = sources.count("EMR".equalsIgnoreCase)
      mainFrames - emrs
    }
  }

  def minusExpenditureUDF: UserDefinedFunction = {
    udf { cell: mutable.WrappedArray[_] =>
      val exps: mutable.Seq[String] = cell.flatMap(el => el.asInstanceOf[GenericRowWithSchema].getValuesMap[String]("expenditure" :: Nil).values)
      if (exps.size == 2) {
        val ints = exps.map(_.toFloat)
        ints.head - ints.tail.head
      } else
        Float.MinValue
    }
  }

  def minusGrpUDF(grpName: String): UserDefinedFunction = {
    udf { cell: mutable.WrappedArray[_] =>
      val exps = cell.flatMap(el => el.asInstanceOf[GenericRowWithSchema].getValuesMap[String](grpName :: Nil).values).toList.filter(_.isEmpty)
      exps match {
        case first :: second :: Nil => Math.max(first.toFloat, second.toFloat) - Math.min(first.toFloat, second.toFloat)
        case _ => Float.MinValue
      }
    }
  }
}
