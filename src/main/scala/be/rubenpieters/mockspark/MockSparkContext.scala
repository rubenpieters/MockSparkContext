package be.rubenpieters.mockspark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by ruben on 2/11/2016.
  */
object MockSparkContext extends SparkContext("local[*]", "local_spark_test", new SparkConf().set("spark.driver.allowMultipleContexts", "true")) {
  override def parallelize[T](seq: Seq[T], numSlices: Int)(implicit evidence$1: ClassManifest[T]): RDD[T] =
    new MockRDD(seq)
}
