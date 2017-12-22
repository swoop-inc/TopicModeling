package org.apache.spark.mllib.topicModeling

object TopicMetrics {

  def round(d: Double, precis: Int = 6): Double = (d * math.pow(10, precis)).toInt / math.pow(10, precis)
}

case class TopicMetrics(topicNum: Int, termWeights: Seq[(String, Double)]) {

  import TopicMetrics._

  override def toString: String = s"Topic-$topicNum: ${termWeights.map{ case (t,w) =>(t,round(w))}.mkString("{"," ","}")}"
  def toCsv = s"${termWeights.map{ case (term,w) => s"$term:${round(w)}"}.mkString(""," ","")}"
}

case class LDAMetrics(testName: String, nDocs: Int, var logLikelihood: Double, var logPerplexity: Double,
  topicMetrics: Seq[TopicMetrics] = Seq.empty[TopicMetrics]) {

  import TopicMetrics._

  logLikelihood = round(logLikelihood,6)

  logPerplexity = round(logPerplexity,6)

}
