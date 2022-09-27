// Función filter

val sc = spark.sparkContext

val rdd = sc.parallelize(1 to 10)

val rddDivisible3 = rdd.filter(_ % 3 == 0)

rddDivisible3.collect

val rddTexto = sc.parallelize(Seq("juan", "julia", "pedro", "katia"))

val rddInicioJ = rddTexto.filter(_.startsWith("j"))

rddInicioJ.collect

val rddInicioJFinA = rddTexto.filter(x => x.startsWith("j") & x.endsWith("a"))

rddInicioJFinA.collect

val rddOrNombre = rddTexto.filter(x => x.startsWith("j") | x.startsWith("k"))

rddOrNombre.collect