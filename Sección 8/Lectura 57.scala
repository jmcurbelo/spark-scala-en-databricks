// Funciones sum, sum_distinct y avg

val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")

vuelos.printSchema

// Nota: desde Spark 3.2.0 sumDistinct quedo deprecado y en su lugar se utiliza sum_distinct

import org.apache.spark.sql.functions.{col, sum, sum_distinct, avg, count}

vuelos.select(
  sum(col("DISTANCE")).as("distancia")
).show

vuelos.select(
  sum_distinct(col("DISTANCE")).as("distancia")
).show

vuelos.select(
  avg(col("AIR_TIME")).as("promedio"),
  (sum(col("AIR_TIME")) / count(col("AIR_TIME"))).as("promedio_manual")
).show
