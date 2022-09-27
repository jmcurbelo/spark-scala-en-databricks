// Funciones para trabajo con strings

val df = spark.read.parquet("/FileStore/section9/strings.parquet")

df.printSchema

df.show(false)

// trim, ltrim, rtrim

import org.apache.spark.sql.functions.{col, trim, ltrim, rtrim}

df.select(
  col("nombre"),
  ltrim(col("nombre")).as("lt"),
  rtrim(col("nombre")).as("rt"),
  trim(col("nombre")).as("tr")
).show(false)

// lpad y rpad

import org.apache.spark.sql.functions.{lpad, rpad}

df.select(
  col("nombre"),
  lpad(trim(col("nombre")), 8, "-").as("lp"),
  rpad(trim(col("nombre")), 8, "=").as("rp")
).show(false)

// concatenación, mayúscula, minúscula y reversa

import org.apache.spark.sql.functions.{concat_ws, lower, upper, initcap, reverse}

df.select(
  concat_ws(" ", col("sujeto"), col("verbo"), col("adjetivo")).as("frase")
).select(
  col("frase"),
  lower(col("frase")).as("lw"),
  upper(col("frase")).as("up"),
  initcap(col("frase")).as("ic"),
  reverse(col("frase")).as("rev")
).show(false)

// regexp_replace

import org.apache.spark.sql.functions.regexp_replace

df.select(
  concat_ws(" ", col("sujeto"), col("verbo"), col("adjetivo")).as("frase")
).select(
  col("frase"),
  regexp_replace(col("frase"), "Spark|es", "lindo").as("regexp")
).show(false)