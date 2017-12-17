package com.blazedb.logging

import java.io.{PrintWriter, StringWriter}

trait Logging {

  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    println("INFO: " + msg)
  }

  protected def logDebug(msg: => String) {
    println(msg)
  }

  protected def logTrace(msg: => String) {
    println(msg)
  }

  protected def logWarning(msg: => String) {
    println("WARN: " + msg)
  }

  protected def logError(msg: => String) {
    println("ERROR: " + msg)
  }

  def toString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    println(msg + ": " + toString(throwable))
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    println(msg + ": " + toString(throwable))
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    println(msg + ": " + toString(throwable))
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    println(msg + ": " + toString(throwable))
  }

  protected def logError(msg: => String, throwable: Throwable) {
    println(msg + ": " + toString(throwable))
  }

}