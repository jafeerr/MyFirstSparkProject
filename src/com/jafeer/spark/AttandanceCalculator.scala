package com.jafeer.spark

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
object AttandanceCalculator {
  val DailyReqHrs = 9 * 60
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named AttandanceCalculator
    val sc = new SparkContext("local[*]", "AttandanceCalculator")

    // Load up each line of the data into an RDD
    val lines = sc.textFile("../ml-100k/report.txt")

    //Parse input swipped hours to minutes eg:  10:00 to 600
    def parseTime(input: String): Long = {
      if (input.isEmpty())
        0
      else {
        val str: Array[String] = input.split(":")
        (str(0).toInt * 60) + (str(1).toInt)
      }
    }

    def formatResult(input: Long): String =
      {
        val builder = new StringBuilder
        val hours: Long = input / 60
        val minutes: Long = input % 60
        if (hours != 0)
          builder.append(hours + " Hours ")
        if (minutes != 0)
          builder.append(minutes + " Minutes")
        builder.toString()
      }
    val info: Array[String] = lines.first().split(",")
    println(s"*************************************\n Employee Id:${info(1)} \tEmployee Name:${info(2)}")
    var attandanceMap = lines.map(x => parseTime(x.toString().split(",")(8))) //swipped hours present in 9th position of CSV
    attandanceMap = attandanceMap.filter((x: Long) => x > 0) //Remove zero swipped hours i.e holidays and weekends

    val requiredMins: Long = (attandanceMap.count() * DailyReqHrs)

    val swippedMins: Long = attandanceMap.sum().toLong
      println(s"No. Of Working Days:${attandanceMap.count()}")
    if (requiredMins > swippedMins)
      println(s" You are lagging mandetory attendance by ${formatResult(requiredMins - swippedMins)}")
    else
      println(s" You have  additional attendance of ${formatResult(swippedMins - requiredMins)}")
    println("*************************************")
  }
}
