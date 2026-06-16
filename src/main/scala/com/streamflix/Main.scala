package com.streamflix

import configuration.Config
import processor.{Modelo1, Modulo2, Modulo3}
import org.apache.spark.sql.SparkSession

import java.util.Scanner

object Main {

  def main(args: Array[String]): Unit = {
    println("Cargando Librerías Spark, por favor espere...")
    val spark = SparkSession.builder().appName("Streamflix").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    println("Librerías cargadas!")

    val pathTxt = Config.SERVER_LOGS_PATH
    val pathCsv = Config.MOVIES_METADATA_CSV

    // CREAR UN MENU DE OPCIONES PARA SELECCIONAR MODULO A EJECUTAR

    val modulo1 = new Modelo1()
//    val modulo2 = new Modulo2()
    val modulo3 = new Modulo3()

    val scc = new Scanner(System.in)

    var corriendo = true
    println("\n¡Bienvenido al sistema StreamFlix!")
    while (corriendo) {3
      println("=== Sistema StreamFlix ===")
      println("1. Módulo 1")
      println("2. Módulo 2")
      println("3. Módulo 3")
      println("4. Módulo 4")
      println("5. Módulo 5")
      println("6. Salir")
      println("\nEs necesario que elija una opción: ")
      val opcion = scc.nextInt()

      opcion match {
        case 1 => modulo1.iniciarModelo1(sc, pathTxt)
        case 2 => Modulo2.iniciarModulo2(spark, sc, pathCsv)
        case 3 => modulo3.iniciarModulo3(spark, pathTxt, pathCsv)
        case 6 =>
          println("Saliendo del sistema...")
          corriendo = false
        case _ => println("Opción no válida...")
      }
    }
  }
}
