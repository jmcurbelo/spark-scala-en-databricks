// Broadcast Variable

val sc = spark.sparkContext

val uno = 1

val brUno = sc.broadcast(uno)

brUno.value

brUno.value + 1

brUno.destroy

brUno.value + 1
