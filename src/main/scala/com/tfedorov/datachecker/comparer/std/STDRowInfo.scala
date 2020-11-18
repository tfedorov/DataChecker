package com.tfedorov.datachecker.comparer.std

import com.tfedorov.datachecker.comparer.RowInfo

case class STDRowInfo(NET_SH_NETWORK: String,
                      TOT_NPG: String,
                      POD_CNT: String,
                      LEN_POD: String,
                      UNIT_POD: String,
                      POD_CNT2: String,
                      LEN_POD2: String,
                      UNIT_POD2: String,
                      POD_CNT3: String,
                      LEN_POD3: String,
                      UNIT_POD3: String,
                      NAT_CML: String,
                      NET_CML: String,
                      BAR_CML: String,
                      NAT_PSA: String,
                      NAT_PRO: String,
                      LOC_NPG: String,
                      LOC_CML: String,
                      LOC_PSA: String,
                      LOC_PRO: String,
                      NAT_CMLPRO: String,
                      NAT_CMLPCT: String,
                      NAT_PROPCT: String,
                      //Extra columns metadata
                      originalSource: String,
                      text: String
                     ) extends RowInfo

object STDRowInfo {

  val baseLineCols: Seq[String] = "net_sh_network" :: Nil
  val _noPodCols: Seq[String] = "TOT_NPG" :: "NAT_CML" :: "NET_CML" :: "BAR_CML" :: "NAT_PSA" :: "NAT_PRO" :: "LOC_NPG" :: "LOC_CML" :: "LOC_PSA" :: "LOC_PRO" :: "NAT_CMLPRO" :: "NAT_CMLPCT" :: "NAT_PROPCT" :: Nil
  val checkedCols: Seq[String] = "TOT_NPG" :: "POD_CNT" :: "LEN_POD" :: "UNIT_POD" :: "POD_CNT2" :: "LEN_POD2" :: "UNIT_POD2" :: "POD_CNT3" :: "LEN_POD3" :: "UNIT_POD3" :: "NAT_CML" :: "NET_CML" :: "BAR_CML" :: "NAT_PSA" :: "NAT_PRO" :: "LOC_NPG" :: "LOC_CML" :: "LOC_PSA" :: "LOC_PRO" :: "NAT_CMLPRO" :: "NAT_CMLPCT" :: "NAT_PROPCT" :: Nil
  val checkedMetaCols: Seq[String] = "TOT_NPG" :: "POD_CNT" :: "LEN_POD" :: "UNIT_POD" :: "POD_CNT2" :: "LEN_POD2" :: "UNIT_POD2" :: "POD_CNT3" :: "LEN_POD3" :: "UNIT_POD3" :: "NAT_CML" :: "NET_CML" :: "BAR_CML" :: "NAT_PSA" :: "NAT_PRO" :: "LOC_NPG" :: "LOC_CML" :: "LOC_PSA" :: "LOC_PRO" :: "NAT_CMLPRO" :: "NAT_CMLPCT" :: "NAT_PROPCT" :: "originalSource" :: "text" :: Nil

}