package com.streamflix.configuration

object Config {

  // Aquí se manejan todas las urls del proyecto

  val SERVER_LOGS_PATH = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/resources/data/server_logs.txt"
  val MOVIES_METADATA_CSV = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/resources/data/movies_metadata.csv"

  val ERROR_COUNTS_TXT_PATH = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/resources/output/error_counts.txt"

  val OUTPUT_MOVIES_PARQUET = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/resources/output/analytics_warehouse"

//  val FICHERO_PARQUET = "C:/Users/ramon.reina/IdeaProjects/streamflix/src/main/resources/output/analytics_warehouse/year=2025/country=DE/.part-00000-a1991aa5-6b8a-4c8c-83c7-44e61d0e5732.c000.snappy.parquet.crc"
}
