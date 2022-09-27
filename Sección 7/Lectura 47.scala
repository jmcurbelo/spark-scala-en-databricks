// Funciones drop, sample y randomSplit

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

// drop

df.printSchema

val dfReducido = df.drop("comments_disabled", "ratings_disabled", "video_error_or_removed")

dfReducido.printSchema

// sample

val dfSample = df.sample(0.9)

val numFilas = df.count

val numSample = dfSample.count

val dfSampleSeed = df.sample(0.9, 1234)

dfSampleSeed.count

val dfSampleReplace = df.sample(true, 0.9, 1234)

dfSampleReplace.count

// randomSplit

val Array(train, test) = df.randomSplit(Array(0.9, 0.1))

println(df.count)

println(train.count)

println(test.count)