package com.ring.testConsumer

import java.util.UUID

import org.apache.commons.logging.LogFactory
import java.util.logging.{Level, Logger}

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.RegionUtils
//import com.ring.testConsumer.CredentialUtils
import com.amazonaws.services.kinesis

/**
  * Created by Karan.Keswani on 3/30/17.
  */
class ScriptsReader {
  private val log = LogFactory.getLog(classOf[ScriptsReader])
  private val ROOT_LOGGER = Logger.getLogger("")
  private val PROCESSOR_LOGGER = Logger.getLogger("com.ring.testConsumer");

  def checkUsage(args: Array[String]): Unit = {
    if(args.length != 3) {
      println(s"Usage ${ScriptsReader.getClass} <application name> <stream name> <region>")
      System.exit(1)
    }
  }

  private def setLogLevels() = {
    ROOT_LOGGER.setLevel(Level.WARNING)
    PROCESSOR_LOGGER.setLevel(Level.INFO)
  }
}

object ScriptsReader extends App {
  var scriptsReader = new ScriptsReader
  scriptsReader.checkUsage(args)

  println("in Script Reader")

  val applicationName = args(0)
  val streamName = args(1)
  val region = RegionUtils.getRegion(args(2))
  if (region == null) {
    System.err.println(args(2) + " is not a valid AWS region.")
    System.exit(1)
  }

  scriptsReader.setLogLevels()

//  val credentialsProvider: AWSCredentialsProvider = CredentialUtils.getCredentialsProvider()
//
//  val workerId = String.valueOf(UUID.randomUUID)
//  val kclConfig = new Nothing(applicationName, streamName, credentialsProvider, workerId).withRegionName(region.getName).withCommonClientConfig(ConfigurationUtils.getClientConfigWithUserAgent)
}