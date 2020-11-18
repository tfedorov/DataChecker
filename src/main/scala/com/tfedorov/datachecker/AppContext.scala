package com.tfedorov.datachecker

case class AppContext(originalInputPath: String,
                      checkedInputPath: String,
                      job: String,
                      columns: String,
                      outputDirPath: Option[String]) {

  var resolvedColumns: Seq[String] = Nil

  def std: AppContext = copy(job = "std")
}

object AppContext {

  def emptyAppArgs = AppContext("", "", "", "_all", None)

  object implicits {

    implicit def tuple2AppArgs(target: (String, String)): AppContext =
      emptyAppArgs.copy(originalInputPath = target._1, checkedInputPath = target._2)
  }

}
