package com.tfedorov.datachecker.comparer.smi

import com.tfedorov.datachecker.comparer.RowInfo

import scala.util.Try

object Parser {
  def originalParse(input: String): Try[SMIRow] =
    Try(splitBrace(input).copy(originalSource = RowInfo.MAINFRAME_SOURCE))

  def checkedParse(input: String): Try[SMIRow] =
    Try(splitBrace(input).copy(originalSource = RowInfo.EMR_SOURCE))

  private def splitBrace(input: String) = {
    val array = input.split("\" \"")
    SMIRow(STATION = array(0).replaceAll("^\"", ""),
      CMCL_DTM = array(1),
      LEN = array(2),
      DAYPART = array(3),
      BROADCAST_DAY = array(4),
      ADVERTISER = array(5),
      SUBSIDIARY = array(6),
      BRAND = array(7),
      PRODUCT = array(8),
      INDUSTRY = array(9),
      MAJOR = array(10),
      PCC_SUB = array(11),
      IS_PROMO = array(12),
      IS_DR = array(13),
      POD = array(14),
      POS = array(15),
      SHOW_TITLE = array(16),
      SHOW_DTM = array(17),
      SHOW_LEN = array(18),
      PROGRAM_TYPE = array(19),
      PROGRAM_SUB_TYPE = array(20),
      AIR_TYPE = array(21),
      RUN_TYPE = array(22),
      EPISODE_TYPE = array(23),
      EVENT_TYPE = array(24),
      SYNDICATION = array(25).trim.replaceAll("\"$", ""),
      //Extra columns metadata
      originalSource = "???",
      text = input)
  }
}
