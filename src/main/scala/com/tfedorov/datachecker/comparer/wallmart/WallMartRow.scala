package com.tfedorov.datachecker.comparer.wallmart

import com.tfedorov.datachecker.comparer.RowInfo

case class WallMartRow(marketCode: String,
                       callLetter: String,
                       affliationCode: String,
                       commercialDate: String,
                       commercialTime: String,
                       ultimateParentDesc: String,
                       brandDesc: String,
                       commercialDescription: String,
                       dayPart: String,
                       commercialDuration: String,
                       commercialType: String,
                       showType: String,
                       tvHouseHold: String,
                       adult_18_49: String,
                       women_18_49: String,
                       adult_35_54: String,
                       men_18_34: String,
                       persons_6_17: String,
                       //Extra columns metadata
                       originalSource: String,
                       text: String
                      ) extends RowInfo {

  private[wallmart] def isQuartOfHour: Boolean =
    ("59" :: "00" :: "01" :: "28" :: "29" :: "30" :: "31" :: Nil).contains(commercialTime.substring(2, 4))

}

object WallMartRow {

  val baseLineCols: Seq[String] = "marketCode" :: "commercialDate" :: "commercialTime" :: "commercialDescription" :: Nil
  val _grps: Seq[String] = "tvHouseHold" :: "adult_18_49" :: "women_18_49" :: "adult_35_54" :: "men_18_34" :: "persons_6_17" :: Nil
  val checkedCols: Seq[String] = "callLetter" :: "affliationCode" :: "ultimateParentDesc" :: "brandDesc" :: "dayPart" :: "commercialDuration" :: "commercialType" :: "showType" :: Nil ++ _grps
}

