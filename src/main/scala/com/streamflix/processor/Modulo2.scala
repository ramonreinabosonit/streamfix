package com.streamflix.processor

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object Modulo2 {

    def iniciarModulo2(path: String) (implicit spark: SparkSession): Unit = {
      val customSchema = StructType(Array(StructField("id", LongType, nullable = false),
        StructField("title", StringType, nullable = false),
        StructField("genres", StringType, nullable = false),
        StructField("subscription_price", StringType, nullable = false),
        StructField("release_date", DateType, nullable = false),
        StructField("country", StringType, nullable = false)
      ))

      // PROBAR EL OPTION CON PERMISSIVE TAMBIEN
      val moviesDF = spark.read.option("header", "true").schema(customSchema).option("mode", "DROPMALFORMED").csv(path)

      val dfFormateado = limpiezaDFPrecioGenero(moviesDF)
      analisisNulosDuplicados(dfFormateado)
      val dfNoNulos = reemplazarNulos(dfFormateado)
      dfNoNulos.show(5)
      dfNoNulos.printSchema()

      // guardar en memoria???
      dfNoNulos.createTempView("moviesDF")
    }

  def limpiezaDFPrecioGenero(moviesDF: DataFrame): DataFrame = {
    // aplicamos primero el split de generos y luego el cast y limpieza del price
    moviesDF.withColumn("genre1", split(col("genres"), "\\|")(0)).withColumn("genre2", split(col("genres"), "\\|")(1))
      .withColumn("subscription_price", regexp_replace(col("subscription_price"), "\\$", "").cast("double"))
  }

  // obtener nulos y duplicados de t odo el df
  def analisisNulosDuplicados(dfFormateado: DataFrame): DataFrame = {
    println("\nHACIENDO ANÁLISIS:")
    dfFormateado.columns.foreach{
      x=>val total = dfFormateado.filter(col(x).isNull).count()
        println(s"$x -> $total nulos")
    }

    println("==========================================")

    // parte de duplicados
    val total = dfFormateado.count()
    val distintos = dfFormateado.distinct().count()
    println(s"Duplicados -> ${total - distintos}")
    // solo hay nulos en genres y en price
    println("\nFIN ANÁLISIS\n")
    dfFormateado
  }

  def reemplazarNulos(dfFormateado: DataFrame): DataFrame = {
    val dfNoNulos = dfFormateado.na.fill(Map("genres" -> "Unknown"))
    dfNoNulos.filter(col("genres").contains("Unknown"))
  }
}
