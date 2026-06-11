package com.streamflix

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.PrintWriter

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Streamflix").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

//    val path = "src/main/resources/data/server_logs.txt"
    val path = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/scala/resources/data/server_logs.txt"
    // cambiarlo para coger rutas de ficheros en un arhcivo de constantes
    val rawLogsRDD = sc.textFile(path)

    val filtradoNivel = rawLogsRDD.filter(x=>x.contains("[ERROR]") || x.contains("[INFO]"))
    println("\nCOUNT:"+filtradoNivel.count())

//  aaaaaa
    println("\nMAPEO (NIVEL, MENSAJE)")
//    val mapeoNivelMensaje =
//    rawLogsRDD.map(x=>x.split(" "))


//    rawLogsRDD.filter(x=>x.contains("[ERROR]")).map(y=>y.split(" ")(0)).take(10).foreach(println)

// Ya cogemos el Nivel, ahora hay que coger el Msj
// [ERROR] 2025-05-16 06:38:41|User:12783|Code:500|Msg:Service Unavailable
    rawLogsRDD.filter(x=>x.contains("ERROR")).
    map {
      cadena =>
        val e = cadena.split(" ")(0)
        // para coger en el string el | hay que poner \\
        val a = cadena.split("\\|")(3)
        (e,a)
    }.take(10).foreach(println)

    // tarea 3
    println("\nCOUNT: "+rawLogsRDD.filter(x=>x.contains("Code:503")).count())

    // tarea 4
    val total = rawLogsRDD.filter(x=>x.contains("INFO") || x.contains("WARN") || x.contains("ERROR")).count()
    val errores = rawLogsRDD.filter(x=>x.contains("ERROR")).count()

    println("Total"+total)
    println(errores)
    println("\nMEDIAAA: "+(errores.toDouble / total) * 100)

    // comprobar si salta la excepcion y cual es para controlarla (collect)

    // CONTAR LINEAS CORRUPTAS
    val countCorruptos = rawLogsRDD.filter(x=>x.split(" ")(0).contains("CORRUPTED")).count()
    println(s"\nCORRUPTOS: $countCorruptos\n")



    // CREAR FICHERO (Codigo, Cantidad)

    val pw = new PrintWriter("C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/scala/resources/output/error_counts.txt")
    // (Codigo, Cantidad)
    val info = rawLogsRDD.filter(x=>x.contains("[INFO]")).count()
    val warn = rawLogsRDD.filter(x=>x.contains("[WARN]")).count()
    val error = rawLogsRDD.filter(x=>x.contains("[ERROR]")).count()

    pw.write(s"(INFO, $info)\n")
    pw.write(s"(WARN, $warn)\n")
    pw.write(s"(ERROR, $error)")

    pw.close()

    println("ESCRITO!")
  }
}
