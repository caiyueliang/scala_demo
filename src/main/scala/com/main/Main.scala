package com.main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

object Main {
  def main(arg: Array[String]) {
    var logger = Logger.getLogger(this.getClass())

    //if (arg.length < 2) {
    //  logger.error("=> wrong parameters number")
    //  System.err.println("Usage: MainExample <path-to-files> <output-path>")
    //  System.exit(1)
    //}

    val jobName = "Main"
    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    println("Hello World")
  }
}
