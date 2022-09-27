// Manejo de nombres de columna duplicados

val empleados= spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show
departamentos.show

val empleadosRen = empleados.withColumnRenamed("num_dpto", "id")

empleadosRen.show

// Si en este punto trataramos de hacer un join como el siguiente nos daría error

import org.apache.spark.sql.functions.col

empleadosRen.join(departamentos, col("id") === col("id")).show

val dfDuplicados = empleadosRen.join(departamentos, empleadosRen.col("id") === departamentos.col("id"))

dfDuplicados.show

import org.apache.spark.sql.functions.col

dfDuplicados.select(col("id")).show

dfDuplicados.select(empleadosRen.col("id")).show

val sinDuplicados = empleadosRen.join(departamentos, Seq("id"))

sinDuplicados.show