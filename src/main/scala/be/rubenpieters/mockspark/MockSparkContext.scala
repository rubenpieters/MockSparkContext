package be.rubenpieters.mockspark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.reflect.ClassTag

/**
  * Created by ruben on 2/11/2016.
  */
object MockSparkContext extends MockitoSugar {
  val sc = mock[SparkContext]
  when(sc.parallelize(any(), any())(any())).thenAnswer(new ParallelizeAnswer)
}

class ParallelizeAnswer[T: ClassTag] extends Answer[RDD[T]] {
  override def answer(invocation: InvocationOnMock): RDD[T] = {
    val args = invocation.getArguments
    new MockRDD(args(0).asInstanceOf[Seq[T]])
  }
}