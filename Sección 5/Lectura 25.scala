// Función reduce

val sc = spark.sparkContext

val rdd = sc.parallelize(1 to 10)

rdd.reduce(_ + _)

10*11/2

val rddP = sc.parallelize(1 to 3)

rddP.reduce(_ * _)