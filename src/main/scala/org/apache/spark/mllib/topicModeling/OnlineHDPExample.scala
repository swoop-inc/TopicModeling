/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.topicModeling

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.mutable


/**
  * An example Online Hierarchical Dirichlet Process (HDP) app. Run with
  * {{{
  * ./bin/run-example mllib.OnlineHDPExample [options] <input>
  * }}}
  */
object OnlineHDPExample {

  case class OnlineHDPParams(
    input: Seq[String] = Seq.empty[String],
    master: String = "local",
    k: Int = 20,
    vocabSize: Int = 10000,
    chunkSize: Int = 256,
    kappa: Double = 1.0,
    tau: Double = 64.0,
    alpha: Double = 1,
    gamma: Double = 1,
    eta: Double = 0.01,
    scale: Double = 1.0,
    var_converge: Double = 0.0001,
    maxIterations: Int = 10,
    checkpointDir: Option[String] = None,
    checkpointInterval: Int = 10,
    maxDocs: Int = -1,
    partitions: Int = 2,
    stopwordFile: String = "",
    logLevel: String = "info"
  ) extends AbstractParams[OnlineHDPParams] {

  }

  def main(args: Array[String]): Unit =  {
    parseAndRun(args)
  }

  def parseAndRun(args: Array[String], inSc: Option[SparkContext] = None): Seq[LDAMetrics] = {

    val parser = getParser(args)
    val results = parser.parse(args, OnlineHDPParams()).map { params=>
      run(params, inSc)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
    results
  }

  def getParser(args: Array[String]) = {
    val defaultParams = OnlineHDPParams()

    val parser = new OptionParser[OnlineHDPParams]("LDAExample") {
      head("LDAExample: an example LDA app for plain text data.")
      opt[String]("master")
        .text(s"spark master ('reuse' if master already started'). default: ${defaultParams.master}")
        .action((x, c) => c.copy(master = x))
      opt[Int]("k")
        .text(s"number of topics. default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))
      opt[Int]("maxDocs")
        .text(s"max number of documents: will truncate the corpus if needbe. default: ${defaultParams.maxDocs}")
        .action((x, c) => c.copy(maxDocs = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      opt[Int]("vocabSize")
        .text(s"number of distinct word types to use, chosen by frequency. (-1=all)" +
          s"  default: ${defaultParams.vocabSize}")
        .action((x, c) => c.copy(vocabSize = x))
      opt[String]("stopwordFile")
        .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
          s"  default: ${defaultParams.stopwordFile}")
        .action((x, c) => c.copy(stopwordFile = x))
      opt[String]("checkpointDir")
        .text(s"Directory for checkpointing intermediate results." +
          s"  Checkpointing helps with recovery and eliminates temporary shuffle files on disk." +
          s"  default: ${defaultParams.checkpointDir}")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"Iterations between each checkpoint.  Only used if checkpointDir is set." +
          s" default: ${defaultParams.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
      opt[Int]("partitions")
        .text(s"Minimum edge partitions, default: ${defaultParams.partitions}")
        .action((x, c) => c.copy(partitions = x))
      opt[String]("logLevel")
        .text(s"Log level, default: ${defaultParams.logLevel}")
        .action((x, c) => c.copy(logLevel = x))
      arg[String]("<input>...")
        .text("input paths (directories) to plain text corpora." +
          "  Each text file line should hold 1 document.")
        .unbounded()
        .required()
        .action((x, c) => c.copy(input = c.input :+ x))
    }
    parser
  }

  def run(params: OnlineHDPParams, inSc: Option[SparkContext] = None) = {

    val sc = inSc.getOrElse {
      val conf = new SparkConf().setAppName(s"LDAExample with $params")
        .setMaster(params.master)
      new SparkContext(conf)
    }

    val logLevel = Level.toLevel(params.logLevel, Level.INFO)
    Logger.getRootLogger.setLevel(logLevel)
    println(s"Setting log level to $logLevel")

    // Load documents, and prepare them for LDA.
    val preprocessStart = System.nanoTime()
    val (corpus, actualCorpusSize, vocabArray, actualNumTokens) =
      preprocess(sc, params.input, params.vocabSize, params.partitions, params.stopwordFile, params.maxDocs)
    val actualVocabSize = vocabArray.size
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9

    println()
    println(s"Corpus summary:")
    println(s"\t Training set size: $actualCorpusSize documents")
    println(s"\t Vocabulary size: $actualVocabSize terms")
    println(s"\t Training set size: $actualNumTokens tokens")
    println(s"\t Preprocessing time: $preprocessElapsed sec")
    println()


    if (params.checkpointDir.nonEmpty) {
      sc.setCheckpointDir(params.checkpointDir.get)
    }

    val startTime = System.nanoTime()
    val lda = new OnlineHDP(corpus, params)
    val results = lda.update(corpus) // Run one honkin big chunk for this example
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training ${getClass.getSimpleName}")
    println(s"Results\n${results.mkString(",")}")
    sc.stop()
    results
  }

  /**
    * Load documents, tokenize them, create vocabulary, and prepare documents as term count vectors.
    *
    * @return (corpus, vocabulary as array, total token count in corpus)
    */
  private def preprocess(
    sc: SparkContext,
    paths: Seq[String],
    vocabSize: Int,
    partitions: Int,
    stopwordFile: String,
    maxDocs: Int): (RDD[(Long, Vector)], Long, Array[String], Long) = {

    // Get dataset of document texts
    // One document per line in each text file. If the input consists of many small files,
    // this can result in a large number of small partitions, which can degrade performance.
    // In this case, consider using coalesce() to create fewer, larger partitions.
    println(s"Loading corpus from ${paths.mkString(",")} ..")
    val textRDD1: RDD[String] = sc.textFile(paths.mkString(","), Math.max(1, partitions))

    val textRDD2 = if (maxDocs == -1) {
      println("Processing all documents in the corpus ..")
      textRDD1
    } else {
      val nDocs = textRDD1.count
      if (nDocs > maxDocs) {
        val sampleFrac = maxDocs.toDouble / nDocs
        println(s"Processing only $maxDocs (out of $nDocs) - ${((sampleFrac*1000).toInt).toDouble/10.0}% ..")
        textRDD1.sample(withReplacement = false, sampleFrac, 1)
      } else {
        textRDD1
      }
    }
    val textRDD = textRDD2.coalesce(partitions)

    // Split text into words
    val tokenizer = new SimpleTokenizer(sc, stopwordFile)
    val tokenized: RDD[(Long, IndexedSeq[String])] = textRDD.zipWithIndex().map { case (text, id) =>
      id -> tokenizer.getWords(text)
    }
    tokenized.cache()

    // Counts words: RDD[(word, wordCount)]
    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()
    val fullVocabSize = wordCounts.count()
    // Select vocab
    //  (vocab: Map[word -> id], total tokens after selecting vocab)
    val (vocab: Map[String, Int], selectedTokenCount: Long) = {
      val tmpSortedWC: Array[(String, Long)] = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2)
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
      }
      (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
    }

    val documents = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc = new mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices = wc.keys.toArray.sorted
      val values = indices.map(i => wc(i).toDouble)

      val sb = Vectors.sparse(vocab.size, indices, values)
      (id, sb)
    }.filter(_._2.asInstanceOf[SparseVector].values.length > 0).cache
    val corpusSize = documents.count
    tokenized.unpersist(false)

    val vocabArray = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabArray(i) = term }

    (documents, corpusSize, vocabArray, selectedTokenCount)
  }
}

