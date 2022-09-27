// Funciones min y max

val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")

vuelos.printSchema

import org.apache.spark.sql.functions.{col, min, max}

vuelos.select(
  min(col("AIR_TIME")).as("min_air_time"),
  max(col("AIR_TIME")).as("max_air_time")
).show

vuelos.describe("AIR_TIME").show

vuelos.select(
  min(col("AIRLINE_DELAY")).as("min_delay"),
  max(col("AIRLINE_DELAY")).as("max_delay")
).show

vuelos.describe("AIRLINE_DELAY").show