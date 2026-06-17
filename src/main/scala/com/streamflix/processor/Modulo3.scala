package com.streamflix.processor
import com.streamflix.configuration.Config
import com.streamflix.processor.Modulo2
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{broadcast, col, explode, split, sum, desc}
//import org.apache.spark.sql.functions._

object Modulo3 {

  def iniciarModulo3(pathTxt: String, pathCsv: String) (implicit spark: SparkSession): Unit = {

    // su columna default es value
    val pruebaDF = spark.read.text(pathTxt)
    pruebaDF.filter(col("value").contains("INFO"))

    // creamos logs DF desde el fichero de logs
    val logsDF = pruebaDF.withColumn("prueba", split(col("value"), "\\|"))
      .withColumn("user_id", split(col("prueba").getItem(1), ":")(1))
      .withColumn("movie_id", split(col("prueba").getItem(2), ":")(1).substr(7, 20))
      .withColumn("duration_watched", split(col("prueba").getItem(3), ":")(1).cast(LongType))
      .select("user_id", "movie_id", "duration_watched")
//    logsDF.show(10)

    // CREAMOS MOVIES DF
    val customSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("genres", StringType, nullable = false),
      StructField("subscription_price", StringType, nullable = false),
      StructField("release_date", DateType, nullable = false),
      StructField("country", StringType, nullable = false)
    ))

    // Leemos de nuevo el DF y aplicamos los métodos hechos en el modulo 2
    // tener cuidado con los DF SON INMUTABLES!
    val moviesDF = spark.read.option("header", "true").schema(customSchema).option("mode", "DROPMALFORMED").csv(pathCsv)
    //    val moviesDFFiltro = Modulo2.limpiezaDFPrecioGenero(moviesDF)
    //    val moviesDFLimpio = Modulo2.reemplazarNulos(moviesDF)

    val enrichedDF = crearJoinLogsMovies(logsDF, moviesDF)

    // obtener géneros y contar horas de las películas
    val genreMetricsDF = enrichedDF.withColumn("genre", explode(split(col("genres"), "\\|")))
      .groupBy("genre").agg(sum("duration_watched").alias("total_hours"))
    println("=== TOP 5 GENEROS MAS VISTOS ===")
    genreMetricsDF.orderBy(desc("total_hours")).show(5)

  }

  def crearJoinLogsMovies(logsDF: DataFrame, moviesDF: DataFrame): DataFrame = {
    // Hacemos el join entre ambos DF mediante las columnas (movie_id, id)
    // se hace broadcast join -->
    logsDF.join(broadcast(moviesDF), logsDF("movie_id") === moviesDF("id"), "inner")
    //    enrichedDF.show(10)
  }

}
