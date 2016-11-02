package be.rubenpieters.example

import be.rubenpieters.mockspark.MockSparkTest
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by ruben on 2/11/2016.
  */
class ExampleTest extends FlatSpec with Matchers with MockSparkTest{
  "mocked spark" should "be fast and easy to use" in {
    sc.parallelize(Seq(1, 2, 3)).map(_ + 1).collect() shouldEqual Seq(2, 3, 4)
  }
}
