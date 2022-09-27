// Acciones sobre un dataframe en Spark SQL

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

df.show()

df.show(5, false)

df.head(1)

df.take(1)

df.select("likes").collect

df.count