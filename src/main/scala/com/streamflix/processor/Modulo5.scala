package com.streamflix.processor

import com.streamflix.configuration.Config
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

object Modulo5 {

  def iniciarModulo5(pathCsv: String)(implicit spark: SparkSession): Unit = {
    val moviesDF = spark.read.option("header", "true").csv(pathCsv)

//    release_date
//    1988-05-30

    println("movies df")
//    val dfNoNulos = dfFormateado.na.fill(Map("genres" -> "Unknown"))
//    dfNoNulos.filter(col("genres").contains("Unknown"))
    val finalReportDF = moviesDF.withColumn("year", split(col("release_date"), "-")(0).cast(LongType))
    finalReportDF.na.fill(Map("year"->1)).show()

    // hacemos el objetivo, transformar y hacer particion de las columnas + dejarlo en formato parquet
//    finalReportDF.write.mode(SaveMode.Overwrite).partitionBy("year", "country").parquet(Config.OUTPUT_MOVIES_PARQUET)

    val ficheroParquet = spark.read.parquet(Config.FICHERO_PARQUET)

    ficheroParquet.show()

    println("FICHERO PARQUET CREADO!")
  }
}
