import org.apache.spark.sql.SparkSession

val sparkS = SparkSession.builder.appName("curso-scala-spark").getOrCreate()

sparkS.conf.getAll.foreach(println)