// Función reduceByKey

val sc = spark.sparkContext

val rdd = sc.parallelize(Seq(
  ("casa", 2),
  ("parque", 1),
  ("que", 5),
  ("casa", 1),
  ("escuela", 2),
  ("casa", 1),
  ("que", 1)
))

rdd.collect

val rddReducido = rdd.reduceByKey(_ + _)

rddReducido.collect

val rddReducido2 = rdd.reduceByKey((x, y) => x + y)

rddReducido2.collect