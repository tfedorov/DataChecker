package com.tfedorov.datachecker.comparer.std

import com.tfedorov.datachecker.comparer.RowInfo

case class STDRow(NET_SH_NETWORK: String,
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
                  NAT_PROPCT: String) {

  private def normailzeTitle(input: String): String = {
    if (input.trim.length > 4)
      input.substring(0, 4).trim
    else
      input.trim
  }


  private def normalizeZeroDate(input: String): String = {
    if (input == null || input.isEmpty)
      return "00:00"
    if (input.trim.indexOf(":") == 1)
      "0" + input.trim
    else
      input.trim
  }

  def toEMRRowInfo: STDRowInfo =
    STDRowInfo(
      NET_SH_NETWORK = this.NET_SH_NETWORK.trim,
      TOT_NPG = this.TOT_NPG.trim,
      POD_CNT = this.POD_CNT.trim,
      LEN_POD = this.LEN_POD.trim,
      UNIT_POD = this.UNIT_POD.trim,
      POD_CNT2 = this.POD_CNT2.trim,
      LEN_POD2 = this.LEN_POD2.trim,
      UNIT_POD2 = this.UNIT_POD2.trim,
      POD_CNT3 = this.POD_CNT3.trim,
      LEN_POD3 = this.LEN_POD3.trim,
      UNIT_POD3 = this.UNIT_POD3.trim,
      NAT_CML = normalizeZeroDate(this.NAT_CML),
      NET_CML = normalizeZeroDate(this.NET_CML.trim),
      BAR_CML = STDRow.timeFix(this.BAR_CML),
      NAT_PSA = normalizeZeroDate(this.NAT_PSA),
      NAT_PRO = normalizeZeroDate(this.NAT_PRO),
      LOC_NPG = normalizeZeroDate(this.LOC_NPG),
      LOC_CML = normalizeZeroDate(this.LOC_CML),
      LOC_PSA = normalizeZeroDate(this.LOC_PSA),
      LOC_PRO = normalizeZeroDate(this.LOC_PRO),
      NAT_CMLPRO = normalizeZeroDate(this.NAT_CMLPRO),
      NAT_CMLPCT = this.NAT_CMLPCT.trim,
      NAT_PROPCT = this.NAT_PROPCT.trim,
      originalSource = RowInfo.EMR_SOURCE,
      text = this.productIterator.map {
        case Some(value) => value
        case None => ""
        case rest => rest
      }.mkString(",")
    )


  def zeroAdd(input: String): String = {
    if (input.trim.indexOf(".") == 1)
      "0" + input.trim
    else
      input.trim
  }

  def toMFRowInfo: STDRowInfo =
    STDRowInfo(
      NET_SH_NETWORK = this.NET_SH_NETWORK.trim,
      TOT_NPG = this.TOT_NPG.trim,
      POD_CNT = this.POD_CNT.trim,
      LEN_POD = this.LEN_POD.trim,
      UNIT_POD = zeroAdd(this.UNIT_POD),
      POD_CNT2 = this.POD_CNT2.trim,
      LEN_POD2 = this.LEN_POD2.trim,
      UNIT_POD2 = zeroAdd(this.UNIT_POD2),
      POD_CNT3 = this.POD_CNT3.trim,
      LEN_POD3 = this.LEN_POD3.trim,
      UNIT_POD3 = zeroAdd(this.UNIT_POD3),
      NAT_CML = this.NAT_CML,
      NET_CML = this.NET_CML.trim,
      BAR_CML = this.BAR_CML.trim,
      NAT_PSA = this.NAT_PSA.trim,
      NAT_PRO = this.NAT_PRO.trim,
      LOC_NPG = this.LOC_NPG.trim,
      LOC_CML = this.LOC_CML.trim,
      LOC_PSA = this.LOC_PSA.trim,
      LOC_PRO = this.LOC_PRO.trim,
      NAT_CMLPRO = this.NAT_CMLPRO.trim,
      NAT_CMLPCT = this.NAT_CMLPCT.trim + ".0",
      NAT_PROPCT = this.NAT_PROPCT.trim + ".0",
      originalSource = RowInfo.MAINFRAME_SOURCE,
      text = this.productIterator.map {
        case Some(value) => value
        case None => ""
        case rest => rest
      }.mkString(",")
    )
}

private[std] object STDRow {
  private val oneNumberPattern = "(\\d{1}):(\\d{2})".r

  private[std] def timeFix(input: String): String = {
    Option(input) match {
      case Some(oneNumberPattern(hour, minus)) => s"0$hour:$minus"
      case None => "00:00"
      case Some(any) => any
    }
  }
}

