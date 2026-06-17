package com.streamflix

import configuration.Config
import org.apache.spark.SparkContext
import processor.{Modelo1, Modulo2, Modulo3, Modulo4}
import org.apache.spark.sql.SparkSession

import java.util.Scanner

// TENGO QUE HACER USO DE RUN
// NO USAR BUCLES NI MENÚ (SOLO PARA TESTEO LOCAL)

object Main {

  // USAR IMPLICIT PARA SPARKSESSION
  // asi todos los metodos lo usan de forma implicita
  def main(args: Array[String]): Unit = {
    println("Cargando Librerías Spark, por favor espere...")

    implicit val spark: SparkSession = SparkSession.builder().appName("Streamflix").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    implicit val sc: SparkContext = spark.sparkContext

    println("Librerías cargadas!")

    val pathTxt = Config.SERVER_LOGS_PATH
    val pathCsv = Config.MOVIES_METADATA_CSV

    val modulo1 = new Modelo1()
//    val modulo2 = new Modulo2()
//    val modulo3 = new Modulo3()

    val scc = new Scanner(System.in)

    var corriendo = true
    println("\n¡Bienvenido al sistema StreamFlix!")
    while (corriendo) {
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
        case 1 => modulo1.iniciarModelo1(pathTxt)
        case 2 => Modulo2.iniciarModulo2(pathCsv)
        case 3 => Modulo3.iniciarModulo3(pathTxt, pathCsv)
        case 4 => Modulo4.iniciarModulo4(pathTxt, pathCsv)
        case 6 =>
          println("Saliendo del sistema...")
          corriendo = false
        case _ => println("Opción no válida...")
      }
    }
  }
}
