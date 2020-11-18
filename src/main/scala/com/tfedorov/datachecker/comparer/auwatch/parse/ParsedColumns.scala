package com.tfedorov.datachecker.comparer.auwatch.parse

private[auwatch] case class ParsedColumns(marketCode: String,
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
                                          maxSequenceWithinPod: String)
