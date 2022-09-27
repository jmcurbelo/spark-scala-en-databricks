dbutils.fs.ls("/FileStore")

/*
/FileStore/section7/dataJSON.json
/FileStore/section7/dataORC.orc
/FileStore/section7/dataPARQUET.parquet
/FileStore/section7/dataTab.txt
/FileStore/section7/dataTXT.txt
/FileStore/section7/dataCSV.csv
*/

// Archivo de texto

val dfTexto = spark.read.text("/FileStore/section7/dataTab.txt")

dfTexto.show()

// Archivo CSV

val dfCSV = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/section7/dataCSV.csv")

dfCSV.show()

dfCSV.printSchema

val dfTextoS = spark.read.option("delimiter", "|").option("header", "true").csv("/FileStore/section7/dataTab.txt")

dfTextoS.show()

// Archivo JSON

val dfJSON = spark.read.json("/FileStore/section7/dataJSON.json")

dfJSON.show()

// Leer una fuente de datos con schema

import org.apache.spark.sql.types._

val schema = StructType(Array(
  StructField("color", StringType, true),
  StructField("edad", IntegerType, true),
  StructField("fecha", DateType, true),
  StructField("pais", StringType, true)
))

val dfConSchema = spark.read.schema(schema).json("/FileStore/section7/dataJSON.json")

dfConSchema.show

dfConSchema.printSchema

// Archivos parquet

val dfParquet = spark.read.parquet("/FileStore/section7/dataPARQUET.parquet")

display(dfParquet)

// archivos ORC

val dfORC = spark.read.orc("/FileStore/section7/dataORC.orc")

display(dfORC)
