package newsProcessor

import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import stanfordLemmatizer.StanfordLemmatizer


class NewsProcessor(dataFrame: DataFrame, bodyColName: String) {
  private val bodyPipe: Pipeline = pipeGen(bodyColName, bodyColName + "Matrix")
  val mainPipe: PipelineModel = new Pipeline().setStages(Array(bodyPipe)).fit(dataFrame)

  private def pipeGen(inputCol: String, outputCol: String): Pipeline = {
    val extraStopwords = Array("say", "would", "one", "make", "like", "get", "go", "also",
      "could", "even", "use", "thing", "way", "see", "l", "var", "el", "", "every")
    val sl = new StanfordLemmatizer()
      .setInputCol(inputCol)
      .setOutputCol(inputCol + "Lemmatized")
    val tokenizer = new RegexTokenizer()
      .setInputCol(inputCol + "Lemmatized")
      .setOutputCol(inputCol + "Tokens")
    val stopWordRemover = new StopWordsRemover()
      .setInputCol(inputCol + "Tokens")
      .setOutputCol(inputCol + "FilteredTokens")
    val stopWordsRemover = stopWordRemover
      .setStopWords(stopWordRemover.getStopWords ++ extraStopwords)
    val countVectorizer = new CountVectorizer()
      .setMinDF(3)
      .setMinTF(1)
      .setInputCol(inputCol + "FilteredTokens")
      .setOutputCol(outputCol)
    new Pipeline().setStages(Array(sl, tokenizer, stopWordsRemover, countVectorizer))
  }
}
