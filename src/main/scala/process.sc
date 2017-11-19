import newsProcessor.NewsProcessor
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder().master("local")
  .appName("Spark SQL basic example")
  .config("master", "spark://myhost:7077")
  .getOrCreate()

val sqlContext = spark.sqlContext

val bwm = spark.read.option("charset", "ascii")
  .text("/Users/warrenronsiek/Projects/coats/data/betweenTheWorldAndMe.txt")

bwm.show()

val p = new NewsProcessor(bwm, "value")
p.mainPipe.write.overwrite()
  .save("/Users/warrenronsiek/Projects/coats/data/coatsProcessor")
p.mainPipe.transform(bwm).write
  .parquet("/Users/warrenronsiek/Projects/coats/data/btwamProcessed.pqt")