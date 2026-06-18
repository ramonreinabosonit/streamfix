package com.streamflix.processor

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{asc, col, concat, concat_ws, desc, expr, lag, split, sum, to_timestamp, unix_timestamp, when}
import org.apache.spark.sql.types.LongType

object Modulo4 {

  // identificar maratones
  // maraton = 3 peliculas consecutivas con menos de 20 minutos de pausa
  // TENER EN CUENTA WATCHING TIME Y LA HORA
  def iniciarModulo4(pathTxt: String, pathCsv: String) (implicit spark: SparkSession): Unit = {

    val pruebaDF = spark.read.text(pathTxt)
    pruebaDF.filter(col("value").contains("INFO"))

    // HAY QUE VELRO ESTOY INTENTANDO CREAR UN DF CON FECHA Y HORA PARA
    // ASI PODER CALCULAR MEJOR LAS MARATONES
    val logsDF =
      pruebaDF.withColumn("prueba1", split(col("value"), " "))
      .withColumn("fecha", col("prueba1").getItem(1))
      .withColumn("hora", split(col("prueba1").getItem(2), "\\|") (0))
        .withColumn("timestamp", to_timestamp(concat_ws(" ", col("fecha"), col("hora"))))
        .withColumn("user_id", split(split(col("prueba1").getItem(2), "\\|")(1), ":")(1).cast(LongType))
        .withColumn("movie_id", split(split(col("prueba1").getItem(2), "\\|")(2), ":")(1).substr(7, 20).cast(LongType))
        .withColumn("duracion", split(split(col("prueba1").getItem(2), "\\|")(3), ":")(1).cast(LongType))
        .select("timestamp", "user_id", "movie_id", "duracion")

    val windowSpec = Window.partitionBy("user_id").orderBy("timestamp")

    // timestamp es la concateninacion de fecha y hora y cremaos la columna timestamp anterior
    // hacemos lo mismo ya no solo con timestamp si no tambien con duracion, cogemos tambien la duracion del anterior
    val lagDF = logsDF.withColumn("prev_timestamp", lag("timestamp", 1).over(windowSpec))
      .withColumn("prev_duracion", lag("duracion", 1).over(windowSpec))
    // los prev_x cogen de la linea anterior, son literalmente el valor de la linea anterior
    // es decir, prev_duracion es null, pero en la segunda linea es el valor duracion de la primera


    // Cogemos lagDF y agregamos la columna descanso minutos que tiene como valor
    // la resta entre el timestamp de la liena actual menos el prev_timestamp que es el valor
    // de la linea anterior, a esto hay que sumarle la duracion de la peli para no perder ese tiempo
    // tod0 esto / 60 (MINUTOS) para obtener minutos
    val descansoDF = lagDF.withColumn("descanso_minutos",
      (unix_timestamp(col("timestamp")) - (unix_timestamp(col("prev_timestamp")) + col("prev_duracion")))/60)

    // en este paso creamos la columan maraton que tendra como valor 0 o 1
    // 0 si supera los 20 minutos y 1 cuando sea inferior
    // esto lo hago asi para poder sumar todos los maratones en el resultadoFinal
    val maratonDF = descansoDF.withColumn("maraton",
      when(col("descanso_minutos") < 20, 1).otherwise(0))

    // aqui mostramos el resultado final en dataframe, seleccionamos los campos que nos interesan
    // y contamos todas las pausas
    val resultadoDF = maratonDF.groupBy("user_id").agg(sum("maraton").alias("total_maratones"))
      .withColumn("is_binge", when(col("total_maratones") >= 3, true).otherwise(false))

    // no salen los ids del inicio, los que tienen maratones como 1001, 3003, etc...
    // resultadoDF.filter(col("is_binge") === true).orderBy(asc("user_id")).show(10)

    // mostrar filter de 3 usuarios con mas maratones
    println("TOP 10 Binge Watchers")
    resultadoDF.orderBy(desc("total_maratones")).show(10)

    println("ID de los 3 ususarios más adictos: ")
    resultadoDF.filter(col("user_id") =!= 999).select("user_id").orderBy(desc("total_maratones")).show(3)

  }

}
