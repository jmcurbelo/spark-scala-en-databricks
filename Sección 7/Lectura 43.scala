// Funciones select y selectExpr

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

df.printSchema

import org.apache.spark.sql.functions.col

df.select(col("video_id")).show()

df.select("video_id", "title").show()

// Esta via da error

df.select(
  "likes",
  "dislikes",
  "likes" - "dislikes"
).show()

//La forma correcta

df.select(
  col("likes"),
  col("dislikes"),
  (col("likes") - col("dislikes")).as("aceptacion")
).show()

// selectExpr

df.selectExpr("likes", "dislikes", "(likes - dislikes) as aceptacion").show()

df.selectExpr("count(distinct(video_id)) as videos").show()