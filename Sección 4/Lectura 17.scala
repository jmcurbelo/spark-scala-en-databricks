// Función flatMap

val sc = spark.sparkContext

val rdd = sc.parallelize(Seq(1,2,3,4,5))

rdd.collect

val rddCuadrado = rdd.map(x => List(x, x * x))

rddCuadrado.collect

val rddCuadradoFlat = rdd.flatMap(x => List(x, x * x))

rddCuadradoFlat.collect

val rddTexto = sc.parallelize(Seq("azul rojo verde", "morado amarillo negro"))

val rddColores = rddTexto.flatMap(_.split(" "))

rddColores.collect