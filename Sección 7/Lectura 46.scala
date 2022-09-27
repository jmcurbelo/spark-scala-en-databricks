// Funciones withColumn y withColumnRenemed

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

// withColumn

import org.apache.spark.sql.functions.col

val dfValoracion = df.withColumn("valoracion", col("likes") - col("dislikes"))

display(dfValoracion)

dfValoracion.printSchema

val dfValoracionCompleja = df.withColumn("valoracion", col("likes") - col("dislikes")).withColumn("resto", col("valoracion") % 10)

display(dfValoracionCompleja)

dfValoracionCompleja.printSchema

// withColumnRenamed

val dfRenombrado = df.withColumnRenamed("video_id", "id")

dfRenombrado.printSchema

// No devuelve error pero no realiza nada por detrás

val test = df.withColumnRenamed("nombrequenoexiste", "nuevonombre")

test.printSchema