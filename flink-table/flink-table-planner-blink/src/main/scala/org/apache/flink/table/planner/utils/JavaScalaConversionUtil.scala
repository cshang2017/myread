package org.apache.flink.table.planner.utils

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}

import java.util.function.{BiConsumer, Consumer, Function}
import java.util.{Optional, List => JList}

import scala.collection.JavaConverters._

/**
  * Utilities for interoperability between Scala and Java classes.
  */
object JavaScalaConversionUtil {

  // most of these methods are not necessary once we upgraded to Scala 2.12

  def toJava[T](option: Option[T]): Optional[T] = option match {
    case Some(v) => Optional.of(v)
    case None => Optional.empty()
  }

  def toScala[T](option: Optional[T]): Option[T] = Option(option.orElse(null.asInstanceOf[T]))

  def toJava[T](func: (T) => Unit): Consumer[T] = new Consumer[T] {
    override def accept(t: T): Unit = {
      func.apply(t)
    }
  }

  def toJava[K, V](func: (K, V) => Unit): BiConsumer[K, V] = new BiConsumer[K, V] {
    override def accept(k: K, v: V): Unit = {
      func.apply(k ,v)
    }
  }

  def toJava[I, O](func: (I) => O): Function[I, O] = new Function[I, O] {
    override def apply(in: I): O = {
      func.apply(in)
    }
  }

  def toJava[T0, T1](tuple: (T0, T1)): JTuple2[T0, T1] = {
    new JTuple2[T0, T1](tuple._1, tuple._2)
  }

  def toJava[T](seq: Seq[T]): JList[T] = {
    seq.asJava
  }

  def toScala[T](list: JList[T]): Seq[T] = {
    list.asScala
  }
}
