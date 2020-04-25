package com.beamsuntory.amz.de.harmonization.keepa

import com.beamsuntory.amz.de.commons.UtilsAmzDE

class HarmonizationKeepaService(val utils: UtilsAmzDE) {

  implicit def anyRef2callable[T >: Null <: AnyRef](klass: T): Caller[T] = Caller(klass)

  case class Caller[T >: Null <: AnyRef](klass: T) {
    def call(methodName: String, args: AnyRef*): AnyRef = {
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes: _*)
      method.invoke(klass, args: _*)
    }
  }

  def parallelExecution(): Unit = {

    val functions = Seq("harmonizationKeepa", "harmonizationHierarchy")
    functions.par.foreach(function =>
      this call function
    )
  }

  def linealExecuction(): Unit = {
    harmonizationKeepa()
    harmonizationHierarchy()
  }

  def harmonizationKeepa(): Unit = new HarmonizationKeepa(utils).mainExecution
  def harmonizationHierarchy(): Unit = new HarmonizationHierarchy(utils).mainExecution

}
