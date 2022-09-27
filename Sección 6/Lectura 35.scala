// Acumuladores

val sc = spark.sparkContext

// longAccumulator

val acumuladorLong = sc.longAccumulator("longAccumulator")

sc.parallelize(1 to 5).foreach(x => acumuladorLong.add(x))

acumuladorLong.reset

acumuladorLong.value

acumuladorLong.isZero

// doubleAccumulator

val acumuladorDouble = sc.doubleAccumulator("doubleAccumulator")

sc.parallelize(1 to 50).foreach(x => acumuladorDouble.add(1))