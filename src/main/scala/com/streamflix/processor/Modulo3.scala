package com.streamflix.processor
import com.streamflix.configuration.Config
import com.streamflix.processor.Modulo2
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{broadcast, col, explode, split, sum, desc}
//import org.apache.spark.sql.functions._

class Modulo3 {

  def iniciarModulo3(spark: SparkSession, pathTxt: String, pathCsv: String): Unit = {

    // su columna default es value
    val pruebaDF = spark.read.text(pathTxt)
    pruebaDF.filter(col("value").contains("INFO"))

    // creamos logs DF desde el fichero de logs
    val logsDF = pruebaDF.withColumn("prueba", split(col("value"), "\\|"))
      .withColumn("user_id", split(col("prueba").getItem(1), ":") (1))
      .withColumn("movie_id", split(col("prueba").getItem(2), ":") (1).substr(7,20))
      .withColumn("duration_watched", split(col("prueba").getItem(3), ":") (1).cast(LongType))
      .select("user_id", "movie_id", "duration_watched")
    logsDF.show(10)

    // CREAMOS MOVIES DF
    val customSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("genres", StringType, nullable = false),
      StructField("subscription_price", StringType, nullable = false),
      StructField("release_date", DateType, nullable = false),
      StructField("country", StringType, nullable = false)
    ))

    // tener cuidado con los DF SON INMUTABLES!
    val moviesDF = spark.read.option("header", "true").schema(customSchema).option("mode", "DROPMALFORMED").csv(pathCsv)
//    val moviesDFFiltro = Modulo2.limpiezaDFPrecioGenero(moviesDF)
//    val moviesDFLimpio = Modulo2.reemplazarNulos(moviesDF)

    // Hacemos el join entre ambos DF mediante las columnas (movie_id, id)
    // se hace broadcast join -->
    val enrichedDF = logsDF.join(broadcast(moviesDF), logsDF("movie_id") === moviesDF("id"), "inner")
    enrichedDF.show(10)

    println("IDS de Logs")
    val logs = logsDF.select(col("movie_id")).distinct().orderBy(desc("movie_id"))
    logs.show()
    val movie = moviesDF.select(col("id")).distinct().orderBy(desc("id"))
    movie.show()

//    println("GENEROS MOVIES")
//    val generos2 = moviesDF.select(col("genres")).distinct()
//    generos2.show()
//    println("GENEROS JOIN")
//    val generos = enrichedDF.select(col("genres")).distinct()
//    generos.show()

    // obtener géneros y contar horas de las películas
    val genreMetricsDF = enrichedDF.withColumn("genre", explode(split(col("genres"), "\\|")))
      .groupBy("genre").agg(sum("duration_watched").alias("total_hours"))

    genreMetricsDF.show(10)
//    mostrarDataFrames(logsDF, moviesDF)

  }

  // PRUEBA
//  def mostrarDataFrames(logsDF: DataFrame, moviesDF: DataFrame): Unit = {
//    logsDF.show(10)
//    moviesDF.show(10)
//  }
}
