// Catalyst Optimizer

val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")

display(vuelos)

import org.apache.spark.sql.functions.col

val nuevoDF = vuelos.filter(col("MONTH")isin(6, 7, 8))
										.withColumn("distancia_tiempo_aire", col("DISTANCE") / col("AIR_TIME"))
										.select(
										  col("AIR_TIME"),
										  col("distancia_tiempo_aire")
										).where(col("AIRLINE").isin("AA", "DL", "AS"))

nuevoDF.explain(true)