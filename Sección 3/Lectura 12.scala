// Diferentes formas de crear un RDD

// Crear un RDD vacío

val sc = spark.sparkContext

val rddVacio = sc.emptyRDD

// Crear un RDD con parallelize

val rddVacio1 = sc.parallelize(Seq(), 3)

rddVacio1.getNumPartitions

val rdd = sc.parallelize(Seq(1,2,3,4,5))

rdd.collect

// Crear un RDD desde un archivo de texto

val rddTexto = sc.textFile("/FileStore/Section3/Lecture12/rdd_source.txt")

rddTexto.collect

val rddTextoCompleto = sc.wholeTextFiles("/FileStore/Section3/Lecture12/rdd_source.txt")

rddTextoCompleto.collect

// Operaciones sencillas con RDD

val rddSuma = rdd.map(x => x + 1)

rddSuma.collect

// Crear un RDD a partir de un DataFrame

import spark.implicits._

val df = Seq((1, "jk"), (2, "ki")).toDF("id", "letras")

df.show()

val rddDataFrame = df.rdd

rddDataFrame.collect