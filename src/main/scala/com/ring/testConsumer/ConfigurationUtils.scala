package com.ring.testConsumer

import com.amazonaws.ClientConfiguration


/**
  * Provides configuration related utilities common to both the producer and consumer.
  */
object ConfigurationUtils {
  private val APPLICATION_NAME = "ring-test-consumer"
  private val VERSION = "1.0.0"

  def getClientConfigWithUserAgent = {
    val config = new ClientConfiguration()
    val userAgent = new StringBuilder(ClientConfiguration.DEFAULT_USER_AGENT)
    // Separate fields of the user agent with a space
    userAgent.append(" ")
    // Append the application name followed by version number of the sample
    userAgent.append(APPLICATION_NAME)
    userAgent.append("/")
    userAgent.append(VERSION)
    config.setUserAgent(userAgent.toString)
    config
  }
}