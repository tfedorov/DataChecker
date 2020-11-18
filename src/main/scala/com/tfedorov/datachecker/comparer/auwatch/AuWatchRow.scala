package com.tfedorov.datachecker.comparer.auwatch

import com.tfedorov.datachecker.comparer.RowInfo

private[auwatch] case class AuWatchRow(marketCode: String,
                                       callLetter: String,
                                       commercialDate: String,
                                       commercialTime: String,
                                       commercialDescription: String,
                                       showTitle: String,
                                       showTypeDescription: String,
                                       dayPart: String,
                                       ultimateParentDesc: String,
                                       parentDesc: String,
                                       brandDesc: String,
                                       pccDesc: String,
                                       commercialDuration: String,
                                       expenditure: String,
                                       podNumber: String,
                                       podSequence: String,
                                       commercialTrafficType: String,
                                       brandVariantDesc: String,
                                       maxSequenceWithinPod: String,
                                       //Extra columns metadata
                                       originalSource: String,
                                       text: String
                                      ) extends RowInfo

private[auwatch] object AuWatchRow {
  val baseLineCols: Seq[String] = "marketCode" :: "commercialDate" :: "commercialTime" :: "ultimateParentDesc" :: Nil
  val checkedCols: Seq[String] = "callLetter" :: "commercialDescription" :: "showTitle" :: "showTypeDescription" :: "dayPart" :: "parentDesc" :: "brandDesc" :: "pccDesc" :: "commercialDuration" :: "expenditure" :: "podNumber" :: "podSequence" :: "commercialTrafficType" :: "brandVariantDesc" :: "maxSequenceWithinPod" :: Nil
}