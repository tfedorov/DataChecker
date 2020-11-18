package com.tfedorov.datachecker.comparer.auwatch.parse

import com.tfedorov.datachecker.comparer.RowInfo
import com.tfedorov.datachecker.comparer.auwatch.AuWatchRow

private[auwatch] object Parser {

  private[auwatch] def checkedParse(input: String): AuWatchRow = {
    val checked: ParsedColumns = splitEMR(input)
    AuWatchRow(
      marketCode = checked.marketCode.trim,
      callLetter = checked.callLetter.trim,
      commercialDate = checked.commercialDate.trim,
      commercialTime = checked.commercialTime.trim,
      commercialDescription = checked.commercialDescription.trim,
      showTitle = checked.showTitle.trim,
      showTypeDescription = checked.showTypeDescription.trim,
      dayPart = checked.dayPart.trim,
      ultimateParentDesc = checked.ultimateParentDesc.trim,
      parentDesc = checked.parentDesc.trim,
      brandDesc = checked.brandDesc.trim,
      pccDesc = checked.pccDesc.trim,
      commercialDuration = checked.commercialDuration.trim,
      expenditure = checked.expenditure.trim,
      podNumber = checked.podNumber.trim,
      podSequence = checked.podSequence.trim,
      commercialTrafficType = checked.commercialTrafficType.trim,
      brandVariantDesc = checked.brandVariantDesc.trim,
      maxSequenceWithinPod = checked.maxSequenceWithinPod.trim,
      originalSource = RowInfo.EMR_SOURCE,
      text = input
    )
  }

  private[auwatch] def originalParse(input: String): AuWatchRow = {
    val mainframe: ParsedColumns = splitThomas(input)
    val dayPartChanged = if (mainframe.dayPart.trim.equalsIgnoreCase("S-S OVERNIGHT")) "S&S OVERNIGHT" else mainframe.dayPart.trim
    AuWatchRow(
      marketCode = mainframe.marketCode.trim,
      callLetter = mainframe.callLetter.trim,
      commercialDate = mainframe.commercialDate.trim,
      commercialTime = mainframe.commercialTime.trim,
      commercialDescription = mainframe.commercialDescription.trim,
      showTitle = mainframe.showTitle.trim,
      showTypeDescription = mainframe.showTypeDescription.trim,
      dayPart = dayPartChanged,
      ultimateParentDesc = mainframe.ultimateParentDesc.trim,
      parentDesc = mainframe.parentDesc.trim,
      brandDesc = mainframe.brandDesc.trim,
      pccDesc = mainframe.pccDesc.trim,
      commercialDuration = mainframe.commercialDuration.trim,
      expenditure = mainframe.expenditure.trim,
      podNumber = mainframe.podNumber.trim,
      podSequence = mainframe.podSequence.trim,
      commercialTrafficType = mainframe.commercialTrafficType.trim,
      brandVariantDesc = mainframe.brandVariantDesc.trim,
      maxSequenceWithinPod = mainframe.maxSequenceWithinPod.trim,
      originalSource = RowInfo.MAINFRAME_SOURCE.trim,
      text = input
    )
  }

  private def splitThomas(line: String): ParsedColumns = {
    if (line.length < 343)
      throw new RuntimeException(s"Could not parse '$line'")
    val marketCode = line.substring(0, 2)
    val callLetter = line.substring(2, 6)
    val commercialDate = line.substring(6, 14)
    val commercialTime = line.substring(14, 20)
    val commercialDescription = line.substring(20, 55)
    val showTitle = line.substring(55, 80)
    val showTypeDescription = line.substring(80, 110)
    val dayPart = line.substring(110, 127)
    val ultimateParentDesc = line.substring(127, 162)
    val parentDesc = line.substring(162, 197)
    val brandDesc = line.substring(197, 262)
    val pccDesc = line.substring(262, 292)
    val commercialDuration = line.substring(292, 295)
    val expenditure = line.substring(295, 303)
    val podNumber = line.substring(303, 306)
    val podSequence = line.substring(306, 309)
    val commercialTrafficType = line.substring(309, 310)
    val brandVariantDesc = line.substring(310, 340)
    val maxSequenceWithinPod = line.substring(340)

    ParsedColumns(marketCode, callLetter, commercialDate, commercialTime,
      commercialDescription, showTitle, showTypeDescription, dayPart, ultimateParentDesc,
      parentDesc, brandDesc, pccDesc, commercialDuration,
      expenditure, podNumber, podSequence, commercialTrafficType, brandVariantDesc, maxSequenceWithinPod)
  }

  private def splitEMR(line: String): ParsedColumns = {

    val marketCode = line.substring(0, 2)
    val callLetter = line.substring(2, 6)
    val commercialDate = line.substring(6, 14)
    val commercialTime = line.substring(14, 20)
    val commercialDescription = line.substring(20, 55)
    val showTitle = line.substring(55, 80)
    val showTypeDescription = line.substring(80, 110)
    val dayPart = line.substring(110, 127)
    val ultimateParentDesc = line.substring(127, 162)
    val parentDesc = line.substring(162, 197)
    val brandDesc = line.substring(197, 262)
    val pccDesc = line.substring(262, 292)
    val commercialDuration = line.substring(292, 295)
    val expenditure = line.substring(295, 303)
    val podNumber = line.substring(303, 306)
    val podSequence = line.substring(306, 309)
    var commercialTrafficType = line.substring(309, 310)
    if (line.length < 337) {
      commercialTrafficType = line.substring(302, 303)
      val brandVariantDesc = line.substring(303, 333)
      val maxSequenceWithinPod = line.substring(333)
      return ParsedColumns(marketCode, callLetter, commercialDate, commercialTime,
        commercialDescription, showTitle, showTypeDescription, dayPart, ultimateParentDesc,
        parentDesc, brandDesc, pccDesc, commercialDuration,
        expenditure, podNumber, podSequence, commercialTrafficType, brandVariantDesc, maxSequenceWithinPod)
    }
    val brandVariantDesc = line.substring(310, 340)
    val maxSequenceWithinPod = line.substring(340)

    ParsedColumns(marketCode, callLetter, commercialDate, commercialTime,
      commercialDescription, showTitle, showTypeDescription, dayPart, ultimateParentDesc,
      parentDesc, brandDesc, pccDesc, commercialDuration,
      expenditure, podNumber, podSequence, commercialTrafficType, brandVariantDesc, maxSequenceWithinPod)
  }
}
