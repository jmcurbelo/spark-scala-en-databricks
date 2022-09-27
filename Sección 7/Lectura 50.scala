// Escritura de DataFrames

val df = spark.read.parquet("/FileStore/section7/lectura44/datos.parquet")

// Escribir el df como csv cambiándole el delimitador

df.write.format("csv").option("sep", "|").save("/FileStore/section7/lectura44/csv")

dbutils.fs.ls("/FileStore/section7/lectura44/csv")

// El número de archivos escritos en el directorio de salida corresponde al número de particiones que tiene un DataFrame

df.rdd.getNumPartitions

df.repartition(2).write.format("csv").option("sep", "|").mode("overwrite").save("/FileStore/section7/lectura44/csv")

dbutils.fs.ls("/FileStore/section7/lectura44/csv")

// Particionando los datos

df.printSchema

import org.apache.spark.sql.functions.col

df.select(col("comments_disabled")).distinct.show

val dfFiltrado = df.filter(col("comments_disabled").isin("False", "True"))

dfFiltrado.write.partitionBy("comments_disabled").parquet("/FileStore/section7/lectura44/parquet")

dbutils.fs.ls("/FileStore/section7/lectura44/parquet")