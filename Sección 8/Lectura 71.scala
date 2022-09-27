// Shuffle Hash Join y Broadcast Hash Join

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show
departamentos.show

import org.apache.spark.sql.functions.{col, broadcast}

empleados.join(broadcast(departamentos), col("num_dpto") === col("id")).show

empleados.join(broadcast(departamentos), col("num_dpto") === col("id")).explain