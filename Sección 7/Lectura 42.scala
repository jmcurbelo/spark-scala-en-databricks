// Trabajo con columnas

val df = spark.read.parquet("/FileStore/section7/dataPARQUET.parquet")

// Primera alternativa para referirse a las columnas

df.printSchema

df.select("title").show()

// Segunda alternativa

import org.apache.spark.sql.functions.{col, column}

df.select(col("title")).show()

df.select(column("title")).show()

// Tercera alternativa

df.select($"title").show()