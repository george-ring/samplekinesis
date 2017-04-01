package com.ring.testKinesis.producer

import java.nio.ByteBuffer

import com.amazonaws.AmazonClientException
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}
import com.ring.model.StockTrade
import com.ring.testKinesis.StockTradeGenerator
import com.ring.utils.{ConfigurationUtils, CredentialUtils}
import org.apache.commons.logging.LogFactory

/**
  * Created by Karan.Keswani on 3/31/17.
  */
class TickerGenerator {

}
object TickerGenerator {
  def checkUsage(args: Array[String]): Unit = {
    if(args.length != 2) {
      println(s"Usage ${classOf[TickerGenerator]} <application name> <stream name> <region>")
      System.exit(1)
    }
  }
  private val LOG = LogFactory.getLog(classOf[TickerGenerator])

  /**
    * Checks if the stream exists and is active
    *
    * @param kinesisClient Amazon Kinesis client instance
    * @param streamName    Name of stream
    */
  def validateStream(kinesisClient: AmazonKinesis, streamName: String): Unit = {
    try {
      val result = kinesisClient.describeStream(streamName)
      if (!("ACTIVE" == result.getStreamDescription.getStreamStatus)) {
        println("Stream " + streamName + " is not active. Please wait a few moments and try again.")
        System.exit(1)
      }
    } catch {
      case e: Exception =>
        System.err.println("Error found while describing the stream " + streamName)
        System.err.println(e)
        System.exit(1)
    }
  }

  def sendStockTrade(trade: StockTrade, kinesisClient: AmazonKinesis, streamName: String): Unit = {
    val bytes = trade.toJsonAsBytes
    // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
    if (bytes == null) {
      LOG.warn("Could not get JSON bytes for stock trade")
      return
    }

    LOG.info("Putting trade: " + trade.toString)
    val putRecord = new PutRecordRequest
    putRecord.setStreamName(streamName)
    // We use the ticker symbol as the partition key, as explained in the tutorial.
    putRecord.setPartitionKey(trade.getTickerSymbol)
    putRecord.setData(ByteBuffer.wrap(bytes))

    try
      kinesisClient.putRecord(putRecord)
    catch {
      case ex: AmazonClientException =>
        LOG.warn("Error sending record to Amazon Kinesis.", ex)
    }
  }

  def main(args: Array[String]): Unit = {
    checkUsage(args)

    val streamName = args(0)
    val regionName = args(1)
    val region = RegionUtils.getRegion(regionName)
    if (region == null) {
      System.err.println(regionName + " is not a valid AWS region.")
      System.exit(1)
    }

    val credentials = CredentialUtils.getCredentialsProvider.getCredentials


    val kinesisClient = new AmazonKinesisClient(credentials, ConfigurationUtils.getClientConfigWithUserAgent)
    kinesisClient.setRegion(region)

    // Validate that the stream exists and is active
    validateStream(kinesisClient, streamName)

    // Repeatedly send stock trades with a 100 milliseconds wait in between
    val stockTradeGenerator = new StockTradeGenerator
    while ({
      true
    }) {
      val trade = stockTradeGenerator.getRandomTrade
      sendStockTrade(trade, kinesisClient, streamName)
      Thread.sleep(100)
    }
  }
}
