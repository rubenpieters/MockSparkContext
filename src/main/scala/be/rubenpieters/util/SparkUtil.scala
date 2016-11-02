package be.rubenpieters.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ruben on 2/11/2016.
  */

object SparkUtil {
  lazy val sparkContext = createRealSparkContext()

  def createRealSparkContext(sparkConf: SparkConf = new SparkConf()): SparkContext = new SparkContext("local[*]", "real_sc", sparkConf)
  def createRealMultSparkContext() = createRealSparkContext(new SparkConf().set("spark.driver.allowMultipleContexts", "true"))
}
