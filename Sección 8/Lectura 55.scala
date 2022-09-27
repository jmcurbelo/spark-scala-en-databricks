// Funciones count, countDistinct y approx_count_distinct

/* /FileStore/section8/muestra.parquet
  /FileStore/section8/vuelos.parquet */

val df = spark.read.parquet("/FileStore/section8/muestra.parquet")

df.show()

// count

import org.apache.spark.sql.functions.{col, count, countDistinct, approx_count_distinct}

df.select(
  count(col("nombre")).as("conteo_nombre"),
  count(col("color")).as("conteo_color")
).show

df.select(
  count(col("nombre")).as("conteo_nombre"),
  count(col("color")).as("conteo_color"),
  count("*").as("conteo_general")
).show

// countDistinct

df.select(
  countDistinct(col("color")).as("conteo_colores_dif")
).show

// approx_count_distinct(col, max_estimated_error=0.05)

val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")

vuelos.printSchema

display(vuelos)

// Realizar un countdistinct y un aprox_count_distinct de las aerolineas

vuelos.select(
  countDistinct(col("AIRLINE")).as("conteo_aerolines"),
  approx_count_distinct(col("AIRLINE")).as("conteo_aprox")
).show
