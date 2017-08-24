package org.dcs.api

object Constants {

  // FIXME: Replace these with dynamic class names of sender types
  val TestSenderClassName = "org.dcs.spark.sender.TestSender"
  val TestFileSenderClassName = "org.dcs.spark.sender.TestFileSender"
  val AccSenderClassName = "org.dcs.spark.sender.AccSender"

  // FIXME: Replace these with dynamic class names of sender types
  val TestReceiverClassName = "org.dcs.spark.receiver.TestReceiver"

  val SparkNameConfKey = "spark.app.name"
  val SparkMasterConfKey = "spark.master"

  val DefaultMaster = "local[2]"
  val DefaultAppName = "AlambeekSparkLocal"

  val SparkPrefix = "spark."

}
