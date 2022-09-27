// Agregación con agrupación

val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")

vuelos.printSchema

import org.apache.spark.sql.functions.{desc, col, avg}

vuelos.groupBy("ORIGIN_AIRPORT").count.orderBy(desc("count")).show

vuelos.groupBy("ORIGIN_AIRPORT", "DESTINATION_AIRPORT").count.orderBy(desc("count")).show