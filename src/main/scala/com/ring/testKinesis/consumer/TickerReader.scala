package com.ring.testKinesis.consumer

import java.util.UUID
import java.util.logging.{Level, Logger}

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.ring.testKinesis.StockTradeRecordProcessorFactory
import com.ring.utils.{ConfigurationUtils, CredentialUtils}
import org.apache.commons.logging.LogFactory
//import com.ring.testConsumer.CredentialUtils

/**
  * Created by Karan.Keswani on 3/30/17.
  */

class TickerReader(){

}
object TickerReader {
  private val log = LogFactory.getLog(classOf[TickerReader])
  private val ROOT_LOGGER = Logger.getLogger("")
  private val PROCESSOR_LOGGER = Logger.getLogger("com.ring.testConsumer");

  def checkUsage(args: Array[String]): Unit = {
    if(args.length != 3) {
      println(s"Usage ${classOf[TickerReader]} <application name> <stream name> <region>")
      System.exit(1)
    }
  }

  private def setLogLevels() = {
    ROOT_LOGGER.setLevel(Level.WARNING)
    PROCESSOR_LOGGER.setLevel(Level.INFO)
  }

  def main(args: Array[String]): Unit = {
    checkUsage(args)

    println("in Script Reader")

    val applicationName = args(0)
    val streamName = args(1)
    val region = RegionUtils.getRegion(args(2))
    if (region == null) {
      System.err.println(args(2) + " is not a valid AWS region.")
      System.exit(1)
    }

    setLogLevels()

    val credentialsProvider: AWSCredentialsProvider = CredentialUtils.getCredentialsProvider

    val workerId = String.valueOf(UUID.randomUUID)
    val kclConfig = new KinesisClientLibConfiguration(applicationName, streamName, credentialsProvider, workerId)
      .withRegionName(region.getName)
      .withCommonClientConfig(ConfigurationUtils.getClientConfigWithUserAgent)

    val recordProcessorFactory = new StockTradeRecordProcessorFactory

    val worker = new Worker(recordProcessorFactory, kclConfig)

    var exitCode = 0
    try
      worker.run()
    catch {
      case t: Throwable =>
        log.error("Caught throwable while processing data.", t)
        exitCode = 1
    }
    System.exit(exitCode)
  }

}