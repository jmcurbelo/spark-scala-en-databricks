// Inner Joins

/* /FileStore/section8/departamentos.parquet
   /FileStore/section8/empleados.parquet */

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show

departamentos.show

// Inner join

import org.apache.spark.sql.functions.col

val joinDF = empleados.join(departamentos, col("num_dpto") === col("id"))

joinDF.show

val joinDF = empleados.join(departamentos, col("num_dpto") === col("id"), "inner")

joinDF.show