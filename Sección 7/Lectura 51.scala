// Persistencia de DataFrames

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

df.persist

df.unpersist

df.cache

df.unpersist

import org.apache.spark.storage.StorageLevel

df.persist(StorageLevel.DISK_ONLY)

df.unpersist

df.persist(StorageLevel.MEMORY_AND_DISK)

df.count