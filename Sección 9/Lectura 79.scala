// Funciones de ventana

val df = spark.read.parquet("/FileStore/section9/funciones_ventana.parquet")

df.show(false)

import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.functions.{desc, row_number, rank, dense_rank}

val windowSpec = Window.partitionBy("departamento").orderBy(desc("puntos"))

df.withColumn("row_number", row_number().over(windowSpec)).show(false)

df1.withColumn("rank", rank().over(windowSpec)).show

df1.withColumn("dense_rank", dense_rank().over(windowSpec)).show

// Agregación con funciones de ventana

val windowSpecAgg = Window.partitionBy("departamento")

import org.apache.spark.sql.functions.{col, min, max, avg}

df.withColumn("min", min(col("puntos")).over(windowSpecAgg))
  .withColumn("max", max(col("puntos")).over(windowSpecAgg))
  .withColumn("avg", avg(col("puntos")).over(windowSpecAgg))
  .withColumn("row_number", row_number().over(windowSpec))
.show