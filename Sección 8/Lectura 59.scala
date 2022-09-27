// Varias agregaciones por grupo

val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")

vuelos.printSchema

import org.apache.spark.sql.functions.{count, min, max, desc, avg}

vuelos.groupBy("ORIGIN_AIRPORT").agg(
  count("AIR_TIME").as("tiempo_aire"),
  min("AIR_TIME").as("min"),
  max("AIR_TIME").as("max")
).orderBy(desc("tiempo_aire")).show

vuelos.groupBy("MONTH").agg(
  count("ARRIVAL_DELAY").as("conteo_retrasos"),
  avg("DISTANCE").as("promedio_distancia")
).orderBy(desc("conteo_retrasos")).show