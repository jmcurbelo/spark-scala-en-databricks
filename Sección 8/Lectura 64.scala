// Left Outer Join

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

import org.apache.spark.sql.functions.col

empleados.show
departamentos.show

empleados.join(departamentos, col("num_dpto") === col("id"), "leftouter").show

empleados.join(departamentos, col("num_dpto") === col("id"), "left_outer").show

empleados.join(departamentos, col("num_dpto") === col("id"), "left").show