package be.rubenpieters.mockspark

import be.rubenpieters.util.SparkUtil
import org.apache.spark.SparkContext
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by ruben on 2/11/2016.
  */
class MockRDDTest extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks with BeforeAndAfterAll {
  val multSparkContext = SparkUtil.createRealSparkContext()

  override protected def afterAll(): Unit = {
    multSparkContext.stop()
  }

  "map" should "behave the same" in {
    forAll { (seq: Seq[Int], f: Int => Int) =>
      checkEqualWithRealSparkContext(sc => sc.parallelize(seq).map(f).collect())
    }
  }

  def checkEqualWithRealSparkContext[A](op: SparkContext => A): Unit = {
    op(multSparkContext) shouldEqual op(MockSparkContext.sc)
  }
}
