// Almacenamiento en caché

val sc = spark.sparkContext

val rdd = sc.parallelize(1 to 10)

import org.apache.spark.storage.StorageLevel

rdd.persist(StorageLevel.MEMORY_ONLY)

rdd.unpersist()

rdd.persist(StorageLevel.DISK_ONLY)

rdd.unpersist()

rdd.cache()