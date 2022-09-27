// Agregación con pivote

val df = spark.read.parquet("/FileStore/section8/estudiantes.parquet")

df.show

df.groupBy("graduacion").pivot("sexo").agg(avg(col("peso"))).show

df.groupBy("graduacion").pivot("sexo").agg(
  avg(col("peso")),
  min(col("peso")),
  max(col("peso"))
).show

df.groupBy("graduacion").pivot("sexo", Array("M")).agg(
  avg(col("peso")),
  min(col("peso")),
  max(col("peso"))
).show