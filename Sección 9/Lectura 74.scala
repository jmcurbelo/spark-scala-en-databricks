// Funciones de fecha y hora

/* /FileStore/section9/calculo.parquet
   /FileStore/section9/convertir.parquet */

// Convirtiendo strings a formato date y timestamp

val df = spark.read.parquet("/FileStore/section9/convertir.parquet")

df.printSchema

df.show(false)

import org.apache.spark.sql.functions.{col, to_date, to_timestamp, date_format}

val dfConvertido = df.select(
  to_date(col("date")).as("date1"),
  to_timestamp(col("timestamp")).as("timestamp1"),
  to_date(col("date"), "yyyy-MM-dd").as("date2"),
  to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS").as("timestamp2")
)

dfConvertido.printSchema

dfConvertido.show(false)

//cambiando el formato de la fecha

dfConvertido.select(date_format(col("date1"), "MM-dd-yyyy")).show(false)

// Ejemplos de cálculo de fecha y hora

val data = spark.read.parquet("/FileStore/section9/calculo.parquet")

data.printSchema

display(data)

import org.apache.spark.sql.functions.{datediff, months_between, last_day}

data.select(
  col("nombre"),
  datediff(col("fecha_salida"), col("fecha_ingreso")).as("dias"),
  months_between(col("fecha_salida"), col("fecha_ingreso")).as("meses"),
  last_day(col("fecha_salida")).as("ultimo_dia_mes")
).show(false)

// Sumando y restando fechas

import org.apache.spark.sql.functions.{date_add, date_sub}

data.select(
  col("nombre"),
  col("fecha_ingreso"),
  date_add(col("fecha_ingreso"), 14).as("mas_14_dias"),
  date_sub(col("fecha_ingreso"), 1).as("menos_1_dia")
).show(false)

// Extrayendo valores especificos

import org.apache.spark.sql.functions.{year, month, weekofyear, dayofmonth, hour, minute, second}

data.select(
  col("nombre"),
  col("baja_sistema"),
  year(col("baja_sistema")),
  month(col("baja_sistema")),
  weekofyear(col("baja_sistema")),
  dayofmonth(col("baja_sistema")),
  hour(col("baja_sistema")),
  minute(col("baja_sistema")),
  second(col("baja_sistema"))
).show(false)