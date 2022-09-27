// Crear un DataFrame a partir de un RDD

val sc = spark.sparkContext

val rdd = sc.parallelize(1 to 10).map(x => (x, x * x))

rdd.collect

import spark.implicits._

val dfSinNombreDeColumnas = rdd.toDF()

dfSinNombreDeColumnas.show()

val dfConNombreDeColumnas = rdd.toDF("numero", "cuadrado")

dfConNombreDeColumnas.show()

// Creando un dataFrame a partir de un RDD con schema
import org.apache.spark.sql.types._

val rddData = sc.parallelize(Seq((1, "jose", 34.5), (2, "julia", 45.9)))

// Transformando el RDD para que contenga instancias de Row

val rddFila = rddData.map(x => Row(x._1, x._2, x._3))

// Creando el schema
// Vía 1
val schema = StructType(Array(
  StructField("id", IntegerType, true),
  StructField("nombre", StringType, true),
  StructField("monto", DoubleType, true)
  )
)

// Vía 2
// La segunda alternativa será crear un esquema a partir de un DLL usando la funcion fromDDL()

val dllSchemaStr = "`id` INT, `nombre` STRING, `monto` DOUBLE"

val ddlSchema = StructType.fromDDL(dllSchemaStr)

// Creando los dataframe

val df1 = spark.createDataFrame(rddFila, schema)

val df2 = spark.createDataFrame(rddFila, ddlSchema)

println("df1")
df1.show()

println("df2")
df2.show()

df1.printSchema
df2.printSchema

//Crear un dataframe a partir de un rango de números

spark.range(5).toDF("id").show()

spark.range(0, 20, 2).toDF("id").show()