import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

import scala.collection.mutable


val spark = SparkSession
  .builder().master("local")
  .appName("Spark SQL basic example")
  .config("master", "spark://myhost:7077")
  .getOrCreate()

val sqlContext = spark.sqlContext

import sqlContext.implicits._

val word2Vec = new Word2Vec()
  .setInputCol("valueFilteredTokens")
  .setOutputCol("result")
  .setVectorSize(3)
  .setMinCount(10)

val btw = spark.read.parquet("/Users/warrenronsiek/Projects/coats/data/btwamProcessed.pqt")

val model = word2Vec.fit(btw)

val result = model.transform(btw)


result.show()
model.findSynonyms("body", 5).show()