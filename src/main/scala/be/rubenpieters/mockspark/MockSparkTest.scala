package be.rubenpieters.mockspark

import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Created by ruben on 2/11/2016.
  */
trait MockSparkTest extends BeforeAndAfterAll { self: Suite =>
  val sc = MockSparkContext

  override protected def afterAll(): Unit = {
    sc.stop()
  }
}
