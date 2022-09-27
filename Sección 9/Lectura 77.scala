// Funciones when, coalesce y lit

// when

val df = spark.read.parquet("/FileStore/section9/lectura71.parquet")

df.show

import org.apache.spark.sql.functions.{col, when, coalesce, lit}

df.select(
  col("nombre"),
  when(col("pago") === 1, "pagado").when(col("pago") === 2, "sin pagar").otherwise("sin iniciar").as("pago")
).show

// coalesce y lit

df.select(
  coalesce(col("nombre"), lit("sin nombre")).as("nombre"),
  col("pago")
).show