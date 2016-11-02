package be.rubenpieters.benchmark

/**
  * Created by ruben on 2/11/2016.
  */
import be.rubenpieters.mockspark.MockSparkContext
import be.rubenpieters.util.SparkUtil
import org.apache.spark.SparkContext
import org.openjdk.jmh.annotations._

class LocalSparkBench {
  // could explain benchmark behaviour of mockito (see point 3):
  // http://stackoverflow.com/a/24992961

  @Benchmark
  def MockedSparkContext(): Unit = {
    op1(MockSparkContext.sc)
  }

  @Benchmark
  def RealSparkContextNotShared(): Unit = {
    op1(SparkUtil.createRealSparkContext())
  }

  @Benchmark
  def RealSparkContextShared(): Unit = {
    op1(SparkUtil.sparkContext)
  }

  def op1(sc: SparkContext): Unit = {
    sc.parallelize(Seq(1, 2, 3), 1).map(_ + 1).collect().toList
  }
}
