package com.tfedorov.datachecker.comparer.smi

import com.tfedorov.datachecker.comparer.RowInfo

case class SMIRow(STATION: String,
                  CMCL_DTM: String,
                  LEN: String,
                  DAYPART: String,
                  BROADCAST_DAY: String,
                  ADVERTISER: String,
                  SUBSIDIARY: String,
                  BRAND: String,
                  PRODUCT: String,
                  INDUSTRY: String,
                  MAJOR: String,
                  PCC_SUB: String,
                  IS_PROMO: String,
                  IS_DR: String,
                  POD: String,
                  POS: String,
                  SHOW_TITLE: String,
                  SHOW_DTM: String,
                  SHOW_LEN: String,
                  PROGRAM_TYPE: String,
                  PROGRAM_SUB_TYPE: String,
                  AIR_TYPE: String,
                  RUN_TYPE: String,
                  EPISODE_TYPE: String,
                  EVENT_TYPE: String,
                  SYNDICATION: String,
                  //Extra columns metadata
                  originalSource: String,
                  text: String
                 ) extends RowInfo

object SMIRow {
  val baseLineCols: Seq[String] = "CMCL_DTM" :: "ADVERTISER" :: Nil
  val checkedCols: Seq[String] = "STATION" :: "LEN" :: "DAYPART" :: "BROADCAST_DAY" :: "SUBSIDIARY" :: "BRAND" :: "PRODUCT" :: "INDUSTRY" :: "MAJOR" :: "PCC_SUB" :: "IS_PROMO" :: "IS_DR" :: "POD" :: "POS" :: "SHOW_TITLE" :: "SHOW_DTM" :: "SHOW_LEN" :: "PROGRAM_TYPE" :: "PROGRAM_SUB_TYPE" :: "AIR_TYPE" :: "RUN_TYPE" :: "EPISODE_TYPE" :: "EVENT_TYPE" :: "SYNDICATION" :: Nil
}