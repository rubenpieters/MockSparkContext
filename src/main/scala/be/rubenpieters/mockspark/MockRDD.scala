package be.rubenpieters.mockspark

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.Map
import scala.reflect.ClassTag

/**
  * Created by ruben on 2/11/2016.
  */
class MockRDD[T: ClassTag](val seq: Seq[T]) extends RDD[T](MockSparkContext, Seq()) {
  // NOTE: max size of the mock is bounded by Int, while normal RDD's can be much larger

  import MockRDD._

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = ???

  override protected def getPartitions: Array[Partition] = ???

  var mockedStorageLevel: StorageLevel = StorageLevel.NONE

  override def persist(newLevel: StorageLevel): MockRDD.this.type = {
    mockedStorageLevel = newLevel
    this
  }

  override def persist(): MockRDD.this.type = {
    mockedStorageLevel = StorageLevel.MEMORY_ONLY
    this
  }

  override def cache(): MockRDD.this.type = {
    mockedStorageLevel = StorageLevel.MEMORY_ONLY
    this
  }

  override def unpersist(blocking: Boolean): MockRDD.this.type = {
    mockedStorageLevel = StorageLevel.NONE
    this
  }

  override def getStorageLevel: StorageLevel = mockedStorageLevel

  override def map[U](f: (T) => U)(implicit evidence$3: ClassManifest[U]): RDD[U] = seq.map(f).toMockRDD

  override def flatMap[U](f: (T) => TraversableOnce[U])(implicit evidence$4: ClassManifest[U]): RDD[U] = seq.flatMap(f).toMockRDD

  override def filter(f: (T) => Boolean): RDD[T] = seq.filter(f).toMockRDD

  override def distinct(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] = distinct()

  override def distinct(): RDD[T] = seq.distinct.toMockRDD

  override def repartition(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] = this

  override def coalesce(numPartitions: Int, shuffle: Boolean)(implicit ord: Ordering[T]): RDD[T] = this

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDD[T] = super.sample(withReplacement, fraction, seed)

  override def randomSplit(weights: Array[Double], seed: Long): Array[RDD[T]] = super.randomSplit(weights, seed)

  override def takeSample(withReplacement: Boolean, num: Int, seed: Long): Array[T] = super.takeSample(withReplacement, num, seed)

  override def union(other: RDD[T]): RDD[T] = ifMockElse(
    other
    , (otherMock: MockRDD[T]) => seq.union(otherMock.seq).toMockRDD
    , (otherRdd: RDD[T]) => otherRdd.union(this)
  )

  override def sortBy[K](f: (T) => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassManifest[K]): RDD[T] = ascending match {
    case true => seq.sortBy(f).toMockRDD
    case false => seq.sortBy(f).reverse.toMockRDD
  }

  override def intersection(other: RDD[T]): RDD[T] = ifMockElse(
    other
    , (otherMock: MockRDD[T]) => intersectMocks(this, otherMock)
    , (otherRdd: RDD[T]) => otherRdd.intersection(this)
  )

  override def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T]): RDD[T] = ifMockElse(
    other
    , (otherMock: MockRDD[T]) => intersectMocks(this, otherMock)
    , (otherRdd: RDD[T]) => otherRdd.intersection(this, partitioner)
  )

  override def intersection(other: RDD[T], numPartitions: Int): RDD[T] = ifMockElse(
    other
    , (otherMock: MockRDD[T]) => intersectMocks(this, otherMock)
    , (otherRdd: RDD[T]) => otherRdd.intersection(this, numPartitions)
  )

  def intersectMocks(mockRdd1: MockRDD[T], mockRdd2: MockRDD[T]): RDD[T] = mockRdd1.seq.intersect(mockRdd2.seq).toMockRDD

  override def glom(): RDD[Array[T]] = Seq(seq.toArray).toMockRDD

  override def cartesian[U](other: RDD[U])(implicit evidence$5: ClassManifest[U]): RDD[(T, U)] = ???

  override def groupBy[K](f: (T) => K)(implicit kt: ClassManifest[K]): RDD[(K, Iterable[T])] = seq.groupBy(f).mapValues(_.toIterable).toSeq.toMockRDD

  override def groupBy[K](f: (T) => K, numPartitions: Int)(implicit kt: ClassManifest[K]): RDD[(K, Iterable[T])] = groupBy(f)

  override def groupBy[K](f: (T) => K, p: Partitioner)(implicit kt: ClassManifest[K], ord: Ordering[K]): RDD[(K, Iterable[T])] = groupBy(f)

  override def pipe(command: String): RDD[String] = ???

  override def pipe(command: String, env: Map[String, String]): RDD[String] = ???

  override def pipe(command: Seq[String], env: Map[String, String], printPipeContext: ((String) => Unit) => Unit, printRDDElement: (T, (String) => Unit) => Unit, separateWorkingDir: Boolean): RDD[String] = ???

  override def mapPartitions[U](f: (Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$6: ClassManifest[U]): RDD[U] = f(seq.toIterator).toSeq.toMockRDD

  override def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$8: ClassManifest[U]): RDD[U] = f(1, seq.toIterator).toSeq.toMockRDD

  @scala.deprecated("use TaskContext.get")
  @DeveloperApi
  override def mapPartitionsWithContext[U](f: (TaskContext, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$9: ClassManifest[U]): RDD[U] = ???

  @scala.deprecated("use mapPartitionsWithIndex")
  override def mapPartitionsWithSplit[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$10: ClassManifest[U]): RDD[U] = ???

  @scala.deprecated("use mapPartitionsWithIndex")
  override def mapWith[A, U](constructA: (Int) => A, preservesPartitioning: Boolean)(f: (T, A) => U)(implicit evidence$11: ClassManifest[U]): RDD[U] = ???

  @scala.deprecated("use mapPartitionsWithIndex and flatMap")
  override def flatMapWith[A, U](constructA: (Int) => A, preservesPartitioning: Boolean)(f: (T, A) => Seq[U])(implicit evidence$12: ClassManifest[U]): RDD[U] = ???

  @scala.deprecated("use mapPartitionsWithIndex and foreach")
  override def foreachWith[A](constructA: (Int) => A)(f: (T, A) => Unit): Unit = ???

  @scala.deprecated("use mapPartitionsWithIndex and filter")
  override def filterWith[A](constructA: (Int) => A)(p: (T, A) => Boolean): RDD[T] = ???

  override def zip[U](other: RDD[U])(implicit evidence$13: ClassManifest[U]): RDD[(T, U)] = ifMockElse(
    other
    , (otherMock: MockRDD[U]) => zipMocks(this, otherMock)
    , (otherRdd: RDD[U]) => otherRdd.zip(this).map{ case (a,b) => (b,a)}
  )

  def zipMocks[U](mockRdd1: MockRDD[T], mockRdd2: MockRDD[U]): RDD[(T, U)] = mockRdd1.seq.zip(mockRdd2.seq).toMockRDD

  override def zipPartitions[B, V](rdd2: RDD[B], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B]) => Iterator[V])(implicit evidence$14: ClassManifest[B], evidence$15: ClassManifest[V]): RDD[V] = ???

  override def zipPartitions[B, V](rdd2: RDD[B])(f: (Iterator[T], Iterator[B]) => Iterator[V])(implicit evidence$16: ClassManifest[B], evidence$17: ClassManifest[V]): RDD[V] = ???

  override def zipPartitions[B, C, V](rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V])(implicit evidence$18: ClassManifest[B], evidence$19: ClassManifest[C], evidence$20: ClassManifest[V]): RDD[V] = ???

  override def zipPartitions[B, C, V](rdd2: RDD[B], rdd3: RDD[C])(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V])(implicit evidence$21: ClassManifest[B], evidence$22: ClassManifest[C], evidence$23: ClassManifest[V]): RDD[V] = ???

  override def zipPartitions[B, C, D, V](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V])(implicit evidence$24: ClassManifest[B], evidence$25: ClassManifest[C], evidence$26: ClassManifest[D], evidence$27: ClassManifest[V]): RDD[V] = ???

  override def zipPartitions[B, C, D, V](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V])(implicit evidence$28: ClassManifest[B], evidence$29: ClassManifest[C], evidence$30: ClassManifest[D], evidence$31: ClassManifest[V]): RDD[V] = ???

  override def foreach(f: (T) => Unit): Unit = seq.foreach(f)

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit = f(seq.toIterator)

  override def collect(): Array[T] = seq.toArray

  override def toLocalIterator: Iterator[T] = seq.toIterator

  override def collect[U](f: PartialFunction[T, U])(implicit evidence$32: ClassManifest[U]): RDD[U] = ???

  override def subtract(other: RDD[T]): RDD[T] = ???

  override def subtract(other: RDD[T], numPartitions: Int): RDD[T] = ???

  override def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T]): RDD[T] = ???

  override def reduce(f: (T, T) => T): T = seq.reduce(f)

  override def treeReduce(f: (T, T) => T, depth: Int): T = ???

  override def fold(zeroValue: T)(op: (T, T) => T): T = seq.fold(zeroValue)(op)

  override def aggregate[U](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)(implicit evidence$33: ClassManifest[U]): U = ???

  override def treeAggregate[U](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U, depth: Int)(implicit evidence$34: ClassManifest[U]): U = ???

  override def count(): Long = seq.size.toLong

  override def countApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] = ???

  override def countByValue()(implicit ord: Ordering[T]): Map[T, Long] = ???

  override def countByValueApprox(timeout: Long, confidence: Double)(implicit ord: Ordering[T]): PartialResult[Map[T, BoundedDouble]] = ???

  override def countApproxDistinct(p: Int, sp: Int): Long = ???

  override def countApproxDistinct(relativeSD: Double): Long = ???

  override def zipWithIndex(): RDD[(T, Long)] = seq.zipWithIndex.map(x => (x._1, x._2.toLong)).toMockRDD

  override def take(num: Int): Array[T] = seq.take(num).toArray

  override def first(): T = super.first()

  override def top(num: Int)(implicit ord: Ordering[T]): Array[T] = super.top(num)

  override def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = super.takeOrdered(num)

  override def max()(implicit ord: Ordering[T]): T = seq.max

  override def min()(implicit ord: Ordering[T]): T = seq.min

  override def isEmpty(): Boolean = seq.isEmpty

  override def saveAsTextFile(path: String): Unit = ???

  override def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = ???

  override def saveAsObjectFile(path: String): Unit = ???

  override def keyBy[K](f: (T) => K): RDD[(K, T)] = ???

  override def checkpoint(): Unit = ???

  override def localCheckpoint(): MockRDD.this.type = ???

  override def isCheckpointed: Boolean = ???

  override def getCheckpointFile: Option[String] = ???

  override def toDebugString: String = ???

  override def toString(): String = s"MockRDD[$seq]"

  override def toJavaRDD(): JavaRDD[T] = ???

  def ifMockElse[A, B](other: RDD[A], ifMock: MockRDD[A] => B, ifNotMock: RDD[A] => B) = {
    other match {
      case mockRdd: MockRDD[A] => ifMock(mockRdd)
      case _ => ifNotMock(other)
    }
  }
}

object MockRDD {
  implicit class seqToMockRDD[A: ClassTag](seq: Seq[A]) {
    def toMockRDD: MockRDD[A] = new MockRDD(seq)
  }
}