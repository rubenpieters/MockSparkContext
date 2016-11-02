package be.rubenpieters.mockspark

import org.scalatest.Suite

/**
  * Created by ruben on 2/11/2016.
  */
trait MockSparkTest { self: Suite =>
  val sc = MockSparkContext
}
