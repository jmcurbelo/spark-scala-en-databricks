// Funciones take, max y saveAsTextFile

val sc = spark.sparkContext

// take
// take: devuelve el registro especificado como argumento.
val rdd = sc.parallelize("La programación es bella".split(" "))

rdd.take(2)

rdd.take(4)

// max 
//max: devuelve el registro máximo.

val rddNumero = sc.parallelize(1 to 15)

rddNumero.max

val rddPar15 = rddNumero.filter(_ % 2 == 0)

rddPar15.max

rddPar15.min

// saveAsTextFile
// saveAsTextFile: usando esta acción, podemos escribir el RDD en un archivo de texto.

rddPar15.saveAsTextFile("/FileStore/Seccion5/Lectura28")

dbutils.fs.ls("/FileStore/Seccion5/Lectura28")

rddPar15.coalesce(1).saveAsTextFile("/FileStore/Seccion5/Lectura28/rdd1")

dbutils.fs.ls("/FileStore/Seccion5/Lectura28/rdd1")