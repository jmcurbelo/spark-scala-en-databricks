// Función repartition

val sc = spark.sparkContext

val rdd = sc.parallelize(1 to 10), 5)

rdd.getNumPartitions

val rdd7 = rdd.repartition(7)

rdd7.getNumPartitions

val rdd3 = rdd.repartition(3)

rdd3.getNumPartitions