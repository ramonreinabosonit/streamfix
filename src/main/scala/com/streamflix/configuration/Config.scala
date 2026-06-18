package com.streamflix.configuration

object Config {

  // Aquí se manejan todas las urls del proyecto

  var SERVER_LOGS_PATH = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/scala/resources/data/server_logs.txt"
  var MOVIES_METADATA_CSV = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/scala/resources/data/movies_metadata.csv"

  var ERROR_COUNTS_TXT_PATH = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/scala/resources/output/error_counts.txt"

  var OUTPUT_MOVIES_PARQUET = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/scala/resources/output/analytics_warehouse"

  var FICHERO_PARQUET = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/scala/resources/output/analytics_warehouse/year=2023/country=US/part-00000-9aa8bc5f-bade-490c-a7da-acdbfd7c58fc.c000.snappy.parquet"
}
