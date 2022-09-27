// Función map

val sc = spark.sparkContext

val rdd = sc.parallelize(Seq(1,2,3,4,5))

val rddResta = rdd.map(_ - 1)

rddResta.collect

val rddPar = rdd.map(_ % 2 == 0)

rddPar.collect

val rddTexto = sc.parallelize(Seq("juan", "pedro", "katia"))

val rddMayuscula = rddTexto.map(_.toUpperCase)

rddMayuscula.collect

val rddHola = rddTexto.map("Hola " + _ + "!")

rddHola.collect
