package com.beamsuntory.amz.de.commons
import org.apache.spark.util.AccumulatorV2

class MyComplex(var x: Int, var y:Int = 0) extends Serializable{
  def reset(): Unit = {
    x = 0
  }
  def add(value: Int): MyComplex = {
    x = x + value
    return this
  }


}

object ComplexAccumulatorV2 extends AccumulatorV2[MyComplex, MyComplex] {

  private val myc:MyComplex = new MyComplex(0)

  def reset(): Unit = {
    myc.reset()
  }

  def add(v: MyComplex): Unit = {
    myc.add(v.x)
  }
  def value():MyComplex = {
    myc
  }
  def isZero(): Boolean = {
    myc.x == 0
  }
  def copy():AccumulatorV2[MyComplex, MyComplex] = {
    ComplexAccumulatorV2
  }

  def merge(other: AccumulatorV2[MyComplex, MyComplex]): Unit = {
    myc.add(other.value.x)
  }
}

