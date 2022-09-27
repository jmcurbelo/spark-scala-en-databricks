// Funciones definidas por el usuario UDF

import org.apache.spark.sql.functions.{col, udf}

val cubo = udf((n: Long) => n*n*n)

val df = spark.range(10)

df.select(
  col("id"),
  cubo(col("id")).as("cubo")
).show