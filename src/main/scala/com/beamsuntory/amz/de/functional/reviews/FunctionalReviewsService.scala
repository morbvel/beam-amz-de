package com.beamsuntory.amz.de.functional.reviews

import com.beamsuntory.amz.de.commons.UtilsAmzDE

class FunctionalReviewsService(val utils: UtilsAmzDE) {

  implicit def anyRef2callable[T>:Null<:AnyRef](klass:T):Caller[T] = Caller(klass)

  case class Caller[T >: Null <: AnyRef](klass: T) {
    def call(methodName: String, args: AnyRef*): AnyRef = {
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes: _*)
      method.invoke(klass, args: _*)
    }
  }

  def execution(option: String): Unit = {

    option match {
      case "p" => parallelExecution()
      case _ => linealExecuction()
    }

  }

  def linealExecuction(): Unit = {
    FunctionalReviews()

  }

  def parallelExecution(): Unit = {

    val functions = Seq("shipmentsExecution", "productExecution")
    functions.par.foreach(function =>
      this call function
    )

  }

  def FunctionalReviews(): Unit = new FunctionalReviews(utils).mainExecution()

}
