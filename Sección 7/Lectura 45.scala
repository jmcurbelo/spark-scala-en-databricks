// Funciones distinct y dropDuplicates

val df = spark.read.parquet("/FileStore/section7/data.parquet")

df.show

df.distinct.show

df.dropDuplicates("id", "color").show

df.dropDuplicates().show