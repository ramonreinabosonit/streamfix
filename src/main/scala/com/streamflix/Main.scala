package com.streamflix

import configuration.Config
import processor.Modelo1

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.PrintWriter

object Main {

  def main(args: Array[String]): Unit = {
    println("Iniciando Main")
    val spark = SparkSession.builder().appName("Streamflix").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val path = Config.SERVER_LOGS_PATH

    val modulo1 = new Modelo1()
    modulo1.iniciarModelo1(sc, path)
  }
}
