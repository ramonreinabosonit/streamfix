package com.streamflix.processor

import com.streamflix.configuration.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection.Schema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, concat_ws, desc, explode, lag, regexp_replace, split, sum, to_timestamp, unix_timestamp, when}
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ETLProcessor {

  // Cambiar logs generos para no reemplazar por Unkown, hacerlo correctamente
  private val logger: Logger = Logger.getLogger(getClass.getName)

  def iniciarProcessor(rawLogsPath: String, moviesCsvPath: String, outputPath: String)(implicit spark: SparkSession): Unit = {

    // Bloque 1, lectura de datos
    val rawLogsDF = readRawLogsWithTry(rawLogsPath)
    val rawMoviesDF = readMoviesWithTry(moviesCsvPath)
    /////////////////////////////////////////////////////////////////

    // Bloque 2, limpieza de datos Logs
    logger.info("Procesando y limpiando los logs raw...")
    val validLogsDF = filterValidLogs(rawLogsDF)
    val cleanLogsDF = parseCleanLogsDF(validLogsDF)
    cleanLogsDF.cache()
    logger.info("Logs de reproducción cargados")

    // Bloque 3, limpiar y estructurar movies
    val moviesLimpioDF = limpiezaDFPrecioGenero(rawMoviesDF)
    val cleanMoviesDF = reemplazarNulos(moviesLimpioDF)
    cleanMoviesDF.cache()
    logger.info("Películas limpias cargadas")
    /////////////////////////////////////////////////////////////////

    // Bloque 4: Enriquecimiento de Datos (Joins)
    logger.info("Enriqueciendo los datos")
//    cleanLogsDF.show()
//    cleanMoviesDF.show()
    val enrichedDF = joinLogsWithMovies(cleanLogsDF, cleanMoviesDF)
    enrichedDF.cache()
    logger.info(s"Resultados del Join: ${enrichedDF.count()} registros")
    /////////////////////////////////////////////////////////////////

    // Bloque 5: Calcular KPIs (Top generos mas vistos y Mostrar los adcitos
    logger.info("Calculando KPI: Géneros más vistos")
    val topGenresDF = explotarColumnaGeneros(enrichedDF)
    topGenresDF.orderBy(desc("total_hours")).select("genre")
//    logger.info(s"aa${topGenresDF.take(5)}")
    // metodo de calculo de tops generos

    logger.info("Calculando KPI: Identificar Binge Watchers")
    val bingeWatchersDF = takeBingeWatchers(cleanLogsDF)
//    bingeWatchersDF.filter(col("is_binge == true")).show(50)
    logger.info(s"Existen ${bingeWatchersDF.select(col("user_id")).distinct().where("is_binge == true").count()} adictos registrados.")
    /////////////////////////////////////////////////////////////////

    // Bloque 6: Guardando los resultados en formato Parquet
    logger.info("Guardando resultados en formato Parquet particionado por año y ciudad")
    writeWarehouse(enrichedDF, outputPath)

    logger.info("FIN DE LA EJECUCION")
  }

  // añadir control de errores TRY CATCH
  def readRawLogsWithTry(logsPath: String)(implicit spark: SparkSession): DataFrame = {
    try {
      val rawLogsDF = spark.read.text(logsPath)
      rawLogsDF

    } catch {
      case e: Exception =>
        logger.error(s"Error leyendo el fichero server_logs.txt:", e)
      throw e
    }
  }

  def readMoviesWithTry(moviesPath: String)(implicit spark: SparkSession): DataFrame = {
    try {
      val moviesDF = spark.read.option("header", "true").schema(createMoviesSchema()).option("mode", "DROPMALFORMED").csv(moviesPath)
      moviesDF
    } catch {
      case e: Exception =>
        logger.error(s"Error leyendo el fichero movies_metadata.csv:", e)
        throw e
    }
  }

  // BLOQUE 2 LIMPIEZA
  def filterValidLogs(rawLogsDF: DataFrame): DataFrame = {
    val validLogsDF = rawLogsDF.filter(col("value").contains("INFO"))
    validLogsDF
  }

  def parseCleanLogsDF(validLogsDF: DataFrame): DataFrame = {
    val logsDF = validLogsDF.withColumn("prueba", split(col("value"), " "))
      .withColumn("fecha", col("prueba").getItem(1))
      .withColumn("hora", split(col("prueba").getItem(2), "\\|") (0))
        .withColumn("timestamp", to_timestamp(concat_ws(" ", col("fecha"), col("hora"))))
        .withColumn("user_id", split(split(col("prueba").getItem(2), "\\|")(1), ":")(1).cast(LongType))
        .withColumn("movie_id", split(split(col("prueba").getItem(2), "\\|")(2), ":")(1).substr(7, 20))
        .withColumn("duration_watched", split(split(col("prueba").getItem(2), "\\|")(3), ":")(1).cast(LongType))
        .select("timestamp","user_id", "movie_id", "duration_watched")
    // para no romper el análisis, descartamos el id 999 que tiene valores disonantes
    val cleanLogsDF = logsDF.filter(col("user_id") =!= 999)
    cleanLogsDF
  }

  def limpiezaDFPrecioGenero(moviesDF: DataFrame): DataFrame = {
    // aplicamos primero el split de generos y luego el cast y limpieza del price
    moviesDF
//      .withColumn("genre1", split(col("genres"), "\\|")(0)).withColumn("genre2", split(col("genres"), "\\|")(1))
      .withColumn("subscription_price", regexp_replace(col("subscription_price"), "\\$", "").cast("double"))
  }

  def reemplazarNulos(dfFormateado: DataFrame): DataFrame = {
    val dfNoNulos = dfFormateado.na.fill(Map("genres" -> "Unknown"))
    dfNoNulos
  }

  // BLOQUE 3
  def joinLogsWithMovies(logsDF: DataFrame, moviesDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val enrichedDF = logsDF.join(broadcast(moviesDF), logsDF("movie_id") === moviesDF("id"), "inner")
    enrichedDF
  }

  // obtener horas por cada genero
  def explotarColumnaGeneros(enrichedDF: DataFrame): DataFrame = {
    val genreMetricsDF = enrichedDF.withColumn("genre", explode(split(col("genres"), "\\|")))
      .groupBy("genre").agg(sum("duration_watched").alias("total_hours"))
    genreMetricsDF
  }

  // BLOQUE 4 (Identificar a los adictos)
  def takeBingeWatchers(logsDF: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("user_id").orderBy("timestamp")

    val lagDF = logsDF.withColumn("prev_timestamp", lag("timestamp", 1).over(windowSpec))
      .withColumn("prev_duration", lag("duration_watched", 1).over(windowSpec))

    val descansosDF = lagDF.withColumn("descanso_minutos",
      (unix_timestamp(col("timestamp")) - (unix_timestamp(col("prev_timestamp")) + col("prev_duration"))) / 60)

    val maratonDF = descansosDF.withColumn("maraton", when(col("descanso_minutos") < 20, 1).otherwise(0))

    val resultadoDF = maratonDF.groupBy("user_id").agg(sum("maraton").alias("total_maratones"))
      .withColumn("is_binge", when(col("total_maratones") >= 3, true).otherwise(false))

//     resultadoDF.filter(col("is_binge") === true).orderBy(desc("total_maratones")).show(10)
    resultadoDF
  }

  // BLOQUE 5 (Escritura en fomrmato Parquet)
  // por año de visualizacion, no por año del csv
  def writeWarehouse(enrichedDF: DataFrame, outputPath: String): Unit = {
//    val finalReportDF = enrichedDF.withColumn("year",split(col("release_date"), "-")(0).cast(LongType))
    val finalReportDF = enrichedDF.withColumn("year", split(col("timestamp"), "-")(0).cast(LongType))
//    enrichedDF.show(10)
    finalReportDF.na.fill(Map("year"->1))

    // escritura
    finalReportDF.write.mode(SaveMode.Overwrite).partitionBy("year", "country").parquet(outputPath)
  }

  // UTILS SUPPORT
  def createMoviesSchema(): StructType = {
    val customSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("genres", StringType, nullable = false),
      StructField("subscription_price", StringType, nullable = false),
      StructField("release_date", DateType, nullable = false),
      StructField("country", StringType, nullable = false)
    ))
    customSchema
  }
}
