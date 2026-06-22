package com.streamflix.processor

import com.streamflix.configuration.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.PrintWriter

class Modelo1 {

  def iniciarModelo1(path: String) (implicit sc: SparkContext): Unit = {

    // cambiarlo para coger rutas de ficheros en un arhcivo de constantes
    val rawLogsRDD = sc.textFile(path)

    tarea1(rawLogsRDD)
    tarea2(rawLogsRDD)
    tarea3(rawLogsRDD)
    tarea4(rawLogsRDD)

    // RESULTADOS POR CONSOLA
    contarCorruptos(rawLogsRDD)
    generarFichero(rawLogsRDD)

    println("FIN Modulo1!")
  }

  def tarea1(rawLogsRDD: RDD[String]): Unit = {
    val filtradoNivel = rawLogsRDD.filter(x=>x.contains("[ERROR]") || x.contains("[INFO]"))
//    println("\nCOUNT:"+filtradoNivel.count())
  }

  def tarea2(rawLogsRDD: RDD[String]): Unit = {
    println("\nMAPEO (NIVEL, MENSAJE)")
    rawLogsRDD.filter(x=>x.contains("ERROR")).
      map {
        cadena =>
          val e = cadena.split(" ")(0)
          // para coger en el string el | hay que poner \\
          val a = cadena.split("\\|")(3)
          (e,a)
      }//.take(10).foreach(println)
  }

  // tarea 3
  def tarea3(rawLogsRDD: RDD[String]): Unit = {
      rawLogsRDD.filter(x=>x.contains("Code:503")).count()
//    println("\nCOUNT: "+rawLogsRDD.filter(x=>x.contains("Code:503")).count())
  }

  // tarea 4
  def tarea4(rawLogsRDD: RDD[String]): Unit = {
    val total = rawLogsRDD.filter(x=>x.contains("INFO") || x.contains("WARN") || x.contains("ERROR")).count()
    val errores = rawLogsRDD.filter(x=>x.contains("ERROR")).count()
    val media = (errores.toDouble / total) * 100

//    println("Total"+total)
//    println(errores)
//    println(s"\nMedia: $media")
  }

  // CONTAR LINEAS CORRUPTAS
  def contarCorruptos(rawLogsRDD: RDD[String]): Unit = {
    val countCorruptos = rawLogsRDD.filter(x=>x.split(" ")(0).contains("CORRUPTED")).count()
    println(s"\nCORRUPTOS: $countCorruptos\n")
  }

  // CREAR FICHERO (Codigo, Cantidad)
  // HAY QUE MODIFICAR ESTO PARA HACERLO CON SPARK, NO PRINTWRITTER
  def generarFichero(rawLogsRDD: RDD[String]): Unit = {
    // habria que usar el saveAsTextFile() con la libreria de DataFrame
    val pw = new PrintWriter(Config.ERROR_COUNTS_TXT_PATH)
    val info = rawLogsRDD.filter(x=>x.contains("[INFO]")).count()
    val warn = rawLogsRDD.filter(x=>x.contains("[WARN]")).count()
    val error = rawLogsRDD.filter(x=>x.contains("[ERROR]")).count()

    pw.write(s"(INFO, $info)\n")
    pw.write(s"(WARN, $warn)\n")
    pw.write(s"(ERROR, $error)")
    println("Fichero Creado!")
    pw.close()
  }

}
